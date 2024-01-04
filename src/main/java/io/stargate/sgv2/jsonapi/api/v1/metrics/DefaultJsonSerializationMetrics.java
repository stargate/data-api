package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class DefaultJsonSerializationMetrics implements JsonSerializationDeserializationMetrics {
  private MeterRegistry meterRegistry;
  private JsonApiMetricsConfig jsonApiMetricsConfig;

  public DefaultJsonSerializationMetrics(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public void addMetrics(Timer.Sample sample, MeterRegistry meterRegistry, String commandName) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag serializationTag = Tag.of(jsonApiMetricsConfig.serializationJson(), "true");
    Tags tags = Tags.of(commandTag, serializationTag);
    sample.stop(meterRegistry.timer(jsonApiMetricsConfig.serializationMetricsName(), tags));
  }
}
