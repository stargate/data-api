package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Timer;

public interface JsonSerializationDeserializationMetrics {
  void addMetrics(Timer.Sample sample, String commandName);
}
