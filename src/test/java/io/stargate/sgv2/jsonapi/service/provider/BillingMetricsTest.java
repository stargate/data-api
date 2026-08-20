package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Guards the meter names and tags — dashboards and alerts key on these exact series. */
class BillingMetricsTest {

  @Test
  void countersFlowToTheExpectedSeries() {
    var registry = new SimpleMeterRegistry();
    var metrics = new BillingMetrics(registry, () -> 0, 100);

    metrics.recordOffered();
    metrics.recordDropped();
    metrics.recordAbandonedAtShutdown(3);
    metrics.recordBatchDelivered(2);
    metrics.recordBatchFailed(5);

    assertThat(registry.counter("billing.s3.events.offered").count()).isEqualTo(1.0);
    assertThat(registry.counter("billing.s3.events.dropped", "reason", "capacity").count())
        .isEqualTo(1.0);
    assertThat(registry.counter("billing.s3.events.dropped", "reason", "shutdown").count())
        .isEqualTo(3.0);
    assertThat(registry.counter("billing.s3.events.flushed").count()).isEqualTo(2.0);
    assertThat(registry.counter("billing.s3.batches.uploaded").count()).isEqualTo(1.0);
    assertThat(registry.counter("billing.s3.events.failed").count()).isEqualTo(5.0);
    assertThat(registry.counter("billing.s3.batches.failed").count()).isEqualTo(1.0);
  }

  @Test
  void depthGaugeReadsTheLiveSupplier() {
    var registry = new SimpleMeterRegistry();
    var depth = new AtomicInteger(7);
    new BillingMetrics(registry, depth::get, 100);

    assertThat(registry.get("billing.s3.queue.depth").gauge().value()).isEqualTo(7.0);
    depth.set(11);
    assertThat(registry.get("billing.s3.queue.depth").gauge().value()).isEqualTo(11.0);
  }

  @Test
  void deliveryHeartbeatAdvancesOnDeliveredBatches() {
    var registry = new SimpleMeterRegistry();
    var metrics = new BillingMetrics(registry, () -> 0, 100);
    var heartbeat = registry.get("billing.s3.last_delivery.epoch_seconds").gauge();

    assertThat(heartbeat.value()).isZero(); // never delivered

    long before = Instant.now().getEpochSecond();
    metrics.recordBatchDelivered(1);
    assertThat(heartbeat.value()).isGreaterThanOrEqualTo(before);
  }
}
