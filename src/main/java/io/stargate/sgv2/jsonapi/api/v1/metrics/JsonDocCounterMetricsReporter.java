package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public class JsonDocCounterMetricsReporter {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private Counter counter;

  public JsonDocCounterMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void createDocCounterMetrics(boolean writeFlag, String commandName) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tags tags = Tags.of(commandTag);

    String metricsName =
        writeFlag
            ? jsonApiMetricsConfig.jsonDocWrittenCounter()
            : jsonApiMetricsConfig.jsonDocReadCounter();
    counter = Counter.builder(metricsName).tags(tags).register(meterRegistry);
  }

  public void increaseDocCounterMetrics(int count) {
    counter.increment(count);
  }
}
