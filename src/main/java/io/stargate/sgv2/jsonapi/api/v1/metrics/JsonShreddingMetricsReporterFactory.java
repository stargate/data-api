package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Injectable factory for creating {@link JsonShreddingMetricsReporter} instances. */
@ApplicationScoped
public class JsonShreddingMetricsReporterFactory {

  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public JsonShreddingMetricsReporterFactory(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public JsonShreddingMetricsReporter jsonShreddingMetricsReporter() {
    return new JsonShreddingMetricsReporter(meterRegistry, jsonApiMetricsConfig);
  }
}
