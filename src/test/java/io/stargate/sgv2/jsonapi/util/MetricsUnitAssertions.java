package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractDoubleAssert;

/** Assertions for working with metrics in **Unit Tests**. */
public class MetricsUnitAssertions {

  private final MeterRegistry meterRegistry;

  public MetricsUnitAssertions() {
    this(new SimpleMeterRegistry());
  }

  public MetricsUnitAssertions(MeterRegistry meterRegistry) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry is null");
  }

  public MeterRegistry registry() {
    return meterRegistry;
  }

  public MetricSnapshot createSnapshot() {

    Map<Meter.Id, Double> values = new HashMap<>();
    for (Meter meter : meterRegistry.getMeters()) {
      switch (meter) {
        case Counter counter -> values.put(meter.getId(), counter.count());
        case Gauge gauge -> values.put(meter.getId(), gauge.value());
        case Timer timer -> values.put(meter.getId(), (double) timer.count());
        case DistributionSummary summary -> values.put(meter.getId(), (double) summary.count());
        default -> {}
      }
    }
    return new MetricSnapshot(values);
  }

  public void assertMetric(Meter metric, double metricValue) {
    assertMetric(null, metric, metricValue);
  }

  public void assertMetric(MetricSnapshot snapshot, Meter metric, double metricValue) {

    var valueAndSnapshot = snapshot == null ? metricValue : snapshot.value(metric) + metricValue;
    assertMetric(metric, (a) -> a.isEqualTo(valueAndSnapshot));
  }

  public void assertMetric(Meter metric, Consumer<AbstractDoubleAssert<?>> assertConsumer) {

    var id = metric.getId();
    var registeredMeter = registry().find(id.getName()).tags(id.getTags()).meter();
    assertThat(registeredMeter)
        .as("assertMetricEqual()- meter is registered. id:" + id)
        .isNotNull();

    AbstractDoubleAssert<?> asserts =
        switch (registeredMeter) {
          case Counter c -> assertThat(c.count());
          case Gauge g -> assertThat(g.value());
          case Timer t -> assertThat((double) t.count());
          default ->
              throw new IllegalArgumentException(
                  "Unknown meter type. id:%s, class:%s".formatted(id, registeredMeter.getClass()));
        };

    var assertsWithDesc = asserts.as("assertMetricEqual()- metric id:" + id);
    assertConsumer.accept(assertsWithDesc);
  }

  public record MetricSnapshot(Map<Meter.Id, Double> values) {

    public double value(Meter meter) {
      return value(meter.getId());
    }

    public double value(Meter.Id meterId) {
      if (values.containsKey(meterId)) {
        return values.get(meterId);
      }
      throw new IllegalArgumentException("Unknown meter id:" + meterId);
    }
  }
}
