package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public class DocJsonCounterMetricsReporter {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private Counter counter;

  public DocJsonCounterMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void createDocCounterMetrics(String commandName) {
    counter =
        Counter.builder(jsonApiMetricsConfig.docJsonCounter())
            .description("The Doc Json counter metrics for '%s'".formatted(commandName))
            .tags(jsonApiMetricsConfig.command(), commandName)
            .register(meterRegistry);
  }

  public void increaseDocCounterMetrics(int count) {
    counter.increment(count);
  }
}
