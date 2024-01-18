package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Configures the {@link JsonShreddingMetricsReporter} instance that going to be injectable and used
 * in the app.
 */
@ApplicationScoped
public class JsonShreddingMetricsFactory {

  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public JsonShreddingMetricsFactory(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public JsonShreddingMetricsReporter jsonShreddingMetricsReporter() {
    return new JsonShreddingMetricsReporter(meterRegistry, jsonApiMetricsConfig);
  }
}
