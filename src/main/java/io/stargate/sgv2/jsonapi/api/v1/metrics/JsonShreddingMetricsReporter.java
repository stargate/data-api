package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

/** Reports the time and size of json shredding. */
public class JsonShreddingMetricsReporter {

  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private Timer.Sample sample;

  public JsonShreddingMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void startMetrics() {
    sample = Timer.start(meterRegistry);
  }

  public void completeMetrics(long totalJsonStringLen, int numberOfDocs, String commandName) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag jsonStringLenTag =
        Tag.of(
            jsonApiMetricsConfig.jsonShreddingSerializedSize(), String.valueOf(totalJsonStringLen));
    Tag numberOfDocTag =
        Tag.of(
            jsonApiMetricsConfig.jsonShreddingSerializedTotalDocuments(),
            String.valueOf(numberOfDocs));
    Tags tags = Tags.of(commandTag, numberOfDocTag, jsonStringLenTag);
    sample.stop(
        meterRegistry.timer(jsonApiMetricsConfig.jsonSerializationPerformanceMetricsName(), tags));
  }
}
