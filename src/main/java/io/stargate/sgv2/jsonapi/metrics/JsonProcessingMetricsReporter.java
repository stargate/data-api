package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
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

  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;

  @Inject
  public JsonProcessingMetricsReporter(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      MetricsConfig metricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    tenantConfig = metricsConfig.tenantRequestCounter();
  }

  public void reportJsonWriteBytesMetrics(Tenant tenant, String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesWritten())
            .tags(getCustomTags(tenant, commandName))
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  public void reportJsonReadBytesMetrics(Tenant tenant, String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesRead())
            .tags(getCustomTags(tenant, commandName))
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  public void reportJsonWrittenDocsMetrics(Tenant tenant, String commandName, int docCount) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonDocsWritten())
            .tags(getCustomTags(tenant, commandName))
            .register(meterRegistry);
    ds.record(docCount);
  }

  public void reportJsonReadDocsMetrics(Tenant tenant, String commandName, int docCount) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonDocsRead())
            .tags(getCustomTags(tenant, commandName))
            .register(meterRegistry);
    ds.record(docCount);
  }

  private Tags getCustomTags(Tenant tenant, String commandName) {
    Tag tenantTag =
        Tag.of(tenantConfig.tenantTag(), tenant == null ? UNKNOWN_VALUE : tenant.toString());
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    return Tags.of(commandTag, tenantTag);
  }
}
