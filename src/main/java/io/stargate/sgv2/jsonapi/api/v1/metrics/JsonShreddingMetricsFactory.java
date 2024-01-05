package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

public class JsonShreddingMetricsFactory {
  @Singleton
  @Produces
  JsonShreddingMetricsReporter jsonShreddingMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    return new JsonShreddingMetricsReporter(meterRegistry, jsonApiMetricsConfig);
  }
}
