package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;

import com.google.common.collect.Lists;
import io.micrometer.core.instrument.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.metrics.MetricsConstants;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a metered wrapper over an {@link EmbeddingProvider}, adding metrics collection to the
 * embedding process. This class is designed to wrap around an existing {@link EmbeddingProvider} to
 * collect and report various metrics, such as the duration of vectorization calls and the size of
 * input texts.
 */
public class MeteredEmbeddingProviderWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(MeteredEmbeddingProviderWrapper.class);

  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final RequestContext requestContext;
  private final EmbeddingProvider embeddingProvider;
  private final String commandName;

  public MeteredEmbeddingProviderWrapper(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      RequestContext requestContext,
      EmbeddingProvider embeddingProvider,
      String commandName) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry);
    this.jsonApiMetricsConfig = Objects.requireNonNull(jsonApiMetricsConfig);
    this.requestContext = Objects.requireNonNull(requestContext);
    this.embeddingProvider = Objects.requireNonNull(embeddingProvider);
    this.commandName = Objects.requireNonNull(commandName);
  }

  /**
   * Vectorizes a list of texts, adding metrics collection for the duration of the vectorization
   * call and the size of the input texts.
   *
   * @param texts the list of texts to vectorize.
   * @param embeddingCredentials the credentials to use for the vectorization call.
   * @param embeddingRequestType the type of embedding request, influencing how texts are processed.
   * @return a {@link Uni} that will provide the list of vectorized texts, as arrays of floats.
   */
  public Uni<EmbeddingProvider.BatchedEmbeddingResponse> vectorize(
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingProvider.EmbeddingRequestType embeddingRequestType) {

    Objects.requireNonNull(texts, "texts must not be null");
    Objects.requireNonNull(embeddingCredentials, "embeddingCredentials must not be null");
    Objects.requireNonNull(embeddingRequestType, "embeddingRequestType type must not be null");

    // String bytes metrics for vectorize
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.vectorizeInputBytesMetrics())
            .tags(getCustomTags())
            .register(meterRegistry);
    texts.stream().mapToInt(String::length).forEach(ds::record);

    // Make batches based on provider batch size
    final List<List<String>> partitions = Lists.partition(texts, maxBatchSize());
    List<Pair<Integer, List<String>>> partitionedBatches = new ArrayList<>(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
      partitionedBatches.add(Pair.of(i, partitions.get(i)));
    }

    // timer metrics for vectorize call
    Timer.Sample sample = Timer.start(meterRegistry);
    Tags tags = getCustomTags();
    // create a multi
    return Multi.createFrom()
        .items(partitionedBatches.stream())
        .onItem()
        .transformToUni(
            batch -> {
              // call vectorize by the batch id
              return embeddingProvider.vectorize(
                  batch.getLeft(), batch.getRight(), embeddingCredentials, embeddingRequestType);
            })
        .merge()
        .collect()
        .asList()
        .onItem()
        .transform(
            vectorizedBatches -> {
              // sort it by batch id, this is required because the merge() run the calls in parallel
              Collections.sort(
                  vectorizedBatches, (a, b) -> Integer.compare(a.batchId(), b.batchId()));
              List<float[]> result = new ArrayList<>();

              ModelUsage aggregatedModelUsage = null;
              for (EmbeddingProvider.BatchedEmbeddingResponse vectorizedBatch : vectorizedBatches) {

                aggregatedModelUsage =
                    aggregatedModelUsage == null
                        ? vectorizedBatch.modelUsage()
                        : aggregatedModelUsage.merge(vectorizedBatch.modelUsage());
                // create the final ordered result
                result.addAll(vectorizedBatch.embeddings());
              }
              var embeddingResponse =
                  new EmbeddingProvider.BatchedEmbeddingResponse(1, result, aggregatedModelUsage);
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Vectorize call completed, aggregatedModelUsage: {}",
                    PrettyPrintable.print(aggregatedModelUsage));
              }
              return embeddingResponse;
            })
        .invoke(
            () ->
                sample.stop(
                    meterRegistry.timer(
                        MetricsConstants.MetricNames.VECTORIZE_CALL_DURATION_METRIC, tags)));
  }

  public int maxBatchSize() {
    final int max = embeddingProvider.maxBatchSize();
    return (max == 0) ? Integer.MAX_VALUE : max;
  }

  /**
   * Generates custom tags for metrics based on the command name, tenant ID, and the name of the
   * embedding provider.
   *
   * @return a collection of {@link Tag} instances for use with metrics.
   */
  private Tags getCustomTags() {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag tenantTag = Tag.of(TENANT_TAG, requestContext.tenant().toString());
    Tag embeddingProviderTag =
        Tag.of(
            jsonApiMetricsConfig.embeddingProvider(), embeddingProvider.getClass().getSimpleName());
    return Tags.of(commandTag, tenantTag, embeddingProviderTag);
  }
}
