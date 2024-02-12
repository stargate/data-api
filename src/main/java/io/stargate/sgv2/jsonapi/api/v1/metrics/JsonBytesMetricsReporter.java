package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JsonBytesMetricsReporter {
  private static final String UNKNOWN_VALUE = "unknown";
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final StargateRequestInfo stargateRequestInfo;
  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;

  @Inject
  public JsonBytesMetricsReporter(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      StargateRequestInfo stargateRequestInfo,
      MetricsConfig metricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.stargateRequestInfo = stargateRequestInfo;
    tenantConfig = metricsConfig.tenantRequestCounter();
  }

  public void reportJsonWriteBytesMetrics(String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesWritten())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  public void reportJsonReadBytesMetrics(String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesRead())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  private Tags getCustomTags(String commandName) {
    Tag tenantTag =
        Tag.of(tenantConfig.tenantTag(), stargateRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    return Tags.of(commandTag, tenantTag);
  }
}
