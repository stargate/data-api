package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.google.common.collect.Lists;
import io.micrometer.core.instrument.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Provides a metered version of an {@link EmbeddingProvider}, adding metrics collection to the
 * embedding process. This class is designed to wrap around an existing {@link EmbeddingProvider} to
 * collect and report various metrics, such as the duration of vectorization calls and the size of
 * input texts.
 */
public class MeteredEmbeddingProvider extends EmbeddingProvider {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final DataApiRequestInfo dataApiRequestInfo;
  private static final String UNKNOWN_VALUE = "unknown";
  private final EmbeddingProvider embeddingProvider;
  private final String commandName;

  public MeteredEmbeddingProvider(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      DataApiRequestInfo dataApiRequestInfo,
      EmbeddingProvider embeddingProvider,
      String commandName) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.embeddingProvider = embeddingProvider;
    this.commandName = commandName;
  }

  /**
   * Vectorizes a list of texts, adding metrics collection for the duration of the vectorization
   * call and the size of the input texts.
   *
   * @param texts the list of texts to vectorize.
   * @param credentials the credentials to use for the vectorization call.
   * @param embeddingRequestType the type of embedding request, influencing how texts are processed.
   * @return a {@link Uni} that will provide the list of vectorized texts, as arrays of floats.
   */
  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Credentials credentials,
      EmbeddingRequestType embeddingRequestType) {
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
                  batch.getLeft(), batch.getRight(), credentials, embeddingRequestType);
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
              for (Response vectorizedBatch : vectorizedBatches) {
                // create the final ordered result
                result.addAll(vectorizedBatch.embeddings());
              }
              return Response.of(1, result);
            })
        .invoke(
            () ->
                sample.stop(
                    meterRegistry.timer(
                        jsonApiMetricsConfig.vectorizeCallDurationMetrics(), tags)));
  }

  @Override
  public int maxBatchSize() {
    return embeddingProvider.maxBatchSize() == 0
        ? Integer.MAX_VALUE
        : embeddingProvider.maxBatchSize();
  }

  /**
   * Generates custom tags for metrics based on the command name, tenant ID, and the name of the
   * embedding provider.
   *
   * @return a collection of {@link Tag} instances for use with metrics.
   */
  private Tags getCustomTags() {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag tenantTag = Tag.of("tenant", dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tag embeddingProviderTag =
        Tag.of(
            jsonApiMetricsConfig.embeddingProvider(), embeddingProvider.getClass().getSimpleName());
    return Tags.of(commandTag, tenantTag, embeddingProviderTag);
  }
}
