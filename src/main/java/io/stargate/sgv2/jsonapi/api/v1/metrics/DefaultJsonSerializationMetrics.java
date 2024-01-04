package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;

public class DefaultJsonSerializationMetrics implements JsonSerializationDeserializationMetrics {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public DefaultJsonSerializationMetrics(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public void addMetrics(Timer.Sample sample, String commandName) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag serializationTag = Tag.of(jsonApiMetricsConfig.jsonProcessTypeTag(), "Serialization");
    Tags tags = Tags.of(commandTag, serializationTag);
    sample.stop(meterRegistry.timer(jsonApiMetricsConfig.jsonProcessMetricsName(), tags));
  }
}
