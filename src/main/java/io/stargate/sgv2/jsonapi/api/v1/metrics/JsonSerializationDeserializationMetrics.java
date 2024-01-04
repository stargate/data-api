package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Timer;
import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "stargate.jsonapi.metric")
public interface JsonSerializationDeserializationMetrics {
  void addMetrics(Timer.Sample sample, String commandName);
}
