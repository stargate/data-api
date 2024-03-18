package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.micrometer.core.instrument.*;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class MeteredEmbeddingProvider implements EmbeddingProvider {
  MeterRegistry meterRegistry;
  JsonApiMetricsConfig jsonApiMetricsConfig;
  DataApiRequestInfo dataApiRequestInfo;
  private static final String UNKNOWN_VALUE = "unknown";
  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;
  EmbeddingProvider embeddingClient;

  String commandName;

  @Inject
  public MeteredEmbeddingProvider(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      DataApiRequestInfo dataApiRequestInfo,
      MetricsConfig metricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.dataApiRequestInfo = dataApiRequestInfo;
    tenantConfig = metricsConfig.tenantRequestCounter();
  }

  public MeteredEmbeddingProvider setEmbeddingClient(
      EmbeddingProvider embeddingClient, String commandName) {
    this.embeddingClient = embeddingClient;
    this.commandName = commandName;
    return this;
  }

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    // timer metrics for vectorize call
    Timer.Sample sample = Timer.start(meterRegistry);
    Uni<List<float[]>> result =
        embeddingClient.vectorize(texts, apiKeyOverride, embeddingRequestType);
    Tags tags = getCustomTags();
    sample.stop(
        meterRegistry.timer(jsonApiMetricsConfig.vectorizeExternalCallDurationMetrics(), tags));

    // String bytes metrics for vectorize
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.vectorizeInputBytesMetrics())
            .tags(getCustomTags())
            .register(meterRegistry);
    texts.stream().mapToInt(String::length).forEach(ds::record);
    return result;
  }

  private Tags getCustomTags() {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag tenantTag =
        Tag.of(tenantConfig.tenantTag(), dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tag embeddingProviderTag =
        Tag.of(
            jsonApiMetricsConfig.embeddingProvider(), embeddingClient.getClass().getSimpleName());
    return Tags.of(commandTag, tenantTag, embeddingProviderTag);
  }
}
