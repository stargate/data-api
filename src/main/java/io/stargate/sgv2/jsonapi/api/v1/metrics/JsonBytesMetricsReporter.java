package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class JsonBytesMetricsReporter {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public JsonBytesMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void createJsonWriteBytesMetrics(String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesWritten())
            .tags(jsonApiMetricsConfig.command(), commandName)
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  public void createJsonReadBytesMetrics(String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesRead())
            .tags(jsonApiMetricsConfig.command(), commandName)
            .register(meterRegistry);
    ds.record(docJsonSize);
  }
}
