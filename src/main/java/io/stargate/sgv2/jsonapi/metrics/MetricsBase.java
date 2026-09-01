package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.*;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Common base for classes that create metric measures */
public abstract class MetricsBase {

  protected final MeterRegistry meterRegistry;
  protected final String prefix;

  protected MetricsBase(MeterRegistry meterRegistry, String prefix) {

    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    this.prefix = Objects.requireNonNull(prefix, "prefix must not be null");
    if (prefix.isBlank()) {
      throw new IllegalArgumentException("prefix must not be blank");
    }
  }

  protected String validateName(String name) {
    Objects.requireNonNull(name, "name must not be null");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }

    return name.charAt(0) == '.' ? name : "." + name;
  }

  protected String fullName(String name) {
    return prefix + validateName(name);
  }

  protected Counter newCounter(String name) {
    return meterRegistry.counter(fullName(name));
  }

  protected Gauge newGauge(String name, Supplier<Number> func) {
    // no null checks in the builder below
    Objects.requireNonNull(func, "func must not be null");

    return Gauge.builder(fullName(name), func).register(meterRegistry);
  }

  protected Timer newTimer(String name) {
    return newTimer(name, 0.5, 0.95, 0.99);
  }

  protected Timer newTimer(String name, double... percentiles) {
    return Timer.builder(fullName(name)).publishPercentiles(percentiles).register(meterRegistry);
  }

  protected TimeGauge newTimeGauge(String name, Supplier<Number> func, TimeUnit unit) {

    return TimeGauge.builder(fullName(name), func, unit).register(meterRegistry);
  }
}
