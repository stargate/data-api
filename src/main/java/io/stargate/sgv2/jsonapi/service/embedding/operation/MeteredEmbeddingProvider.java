package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.micrometer.core.instrument.*;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import java.util.List;
import java.util.Optional;

/**
 * Provides a metered version of an {@link EmbeddingProvider}, adding metrics collection to the
 * embedding process. This class is designed to wrap around an existing {@link EmbeddingProvider} to
 * collect and report various metrics, such as the duration of vectorization calls and the size of
 * input texts.
 */
public class MeteredEmbeddingProvider implements EmbeddingProvider {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final DataApiRequestInfo dataApiRequestInfo;
  private static final String UNKNOWN_VALUE = "unknown";
  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;
  private final EmbeddingProvider embeddingProvider;
  private final String commandName;

  public MeteredEmbeddingProvider(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      DataApiRequestInfo dataApiRequestInfo,
      MetricsConfig metricsConfig,
      EmbeddingProvider embeddingProvider,
      String commandName) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.dataApiRequestInfo = dataApiRequestInfo;
    tenantConfig = metricsConfig.tenantRequestCounter();
    this.embeddingProvider = embeddingProvider;
    this.commandName = commandName;
  }

  /**
   * Vectorizes a list of texts, adding metrics collection for the duration of the vectorization
   * call and the size of the input texts.
   *
   * @param texts the list of texts to vectorize.
   * @param apiKeyOverride optional API key to override any default authentication mechanism.
   * @param embeddingRequestType the type of embedding request, influencing how texts are processed.
   * @return a {@link Uni} that will provide the list of vectorized texts, as arrays of floats.
   */
  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    // String bytes metrics for vectorize
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.vectorizeInputBytesMetrics())
            .tags(getCustomTags())
            .register(meterRegistry);
    texts.stream().mapToInt(String::length).forEach(ds::record);

    // timer metrics for vectorize call
    Timer.Sample sample = Timer.start(meterRegistry);
    Tags tags = getCustomTags();
    return embeddingProvider
        .vectorize(texts, apiKeyOverride, embeddingRequestType)
        .invoke(
            () ->
                sample.stop(
                    meterRegistry.timer(
                        jsonApiMetricsConfig.vectorizeCallDurationMetrics(), tags)));
  }

  /**
   * Generates custom tags for metrics based on the command name, tenant ID, and the name of the
   * embedding provider.
   *
   * @return a collection of {@link Tag} instances for use with metrics.
   */
  private Tags getCustomTags() {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag tenantTag =
        Tag.of(tenantConfig.tenantTag(), dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tag embeddingProviderTag =
        Tag.of(
            jsonApiMetricsConfig.embeddingProvider(), embeddingProvider.getClass().getSimpleName());
    return Tags.of(commandTag, tenantTag, embeddingProviderTag);
  }
}
