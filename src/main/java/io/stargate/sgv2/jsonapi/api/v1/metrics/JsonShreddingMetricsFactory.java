package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/**
 * Configures the {@link JsonShreddingMetricsReporter} instance that going to be injectable and used
 * in the app.
 */
public class JsonShreddingMetricsFactory {
  /** Replaces the CDI producer for JsonShreddingMetricsReporter built into Quarkus. */
  @Singleton
  @Produces
  JsonShreddingMetricsReporter jsonShreddingMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    return new JsonShreddingMetricsReporter(meterRegistry, jsonApiMetricsConfig);
  }
}
