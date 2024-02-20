package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Reports metrics related to JSON byte sizes and operation counts for various commands. Utilizes
 * Micrometer's {@link MeterRegistry} for metric registration and reporting, allowing integration
 * with various monitoring systems. Metrics include JSON bytes written/read and counts of JSON
 * write/read operations, tagged with command and tenant information.
 */
@ApplicationScoped
public class JsonProcessingMetricsReporter {
  private static final String UNKNOWN_VALUE = "unknown";
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final DataApiRequestInfo dataApiRequestInfo;
  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;

  @Inject
  public JsonProcessingMetricsReporter(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      DataApiRequestInfo dataApiRequestInfo,
      MetricsConfig metricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.dataApiRequestInfo = dataApiRequestInfo;
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

  public void reportJsonWrittenDocsMetrics(String commandName, int docCount) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonDocsWritten())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docCount);
  }

  public void reportJsonReadDocsMetrics(String commandName, int docCount) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonDocsRead())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docCount);
  }

  private Tags getCustomTags(String commandName) {
    Tag tenantTag =
        Tag.of(tenantConfig.tenantTag(), dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    return Tags.of(commandTag, tenantTag);
  }
}
