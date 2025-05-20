package io.stargate.sgv2.jsonapi.service.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.metrics.MetricsTenantDeactivationConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsTenantDeactivationConsumerTests {
  MeterRegistry meterRegistry;
  MetricsTenantDeactivationConsumer consumer;

  @BeforeEach
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    consumer = new MetricsTenantDeactivationConsumer(meterRegistry);
  }

  @AfterEach
  public void tearDown() {
    Metrics.globalRegistry.clear();
    if (meterRegistry != null) {
      meterRegistry.clear();
      meterRegistry.close();
    }
  }

  @Test
  public void storeMeterIdForTenant() {
    String tenantId = "tenant1";
    Meter.Id meterId1 =
        Timer.builder("my.timer")
            .tag("command", "test1")
            .tag("tenant", tenantId)
            .register(meterRegistry)
            .getId();
    Meter.Id meterId2 =
        Timer.builder("my.timer")
            .tag("command", "test2")
            .tag("tenant", tenantId)
            .register(meterRegistry)
            .getId();

    consumer.trackMeterId(tenantId, meterId1);
    consumer.trackMeterId(tenantId, meterId2);

    assertThat(consumer.getTenantMetrics().get(tenantId))
        .containsExactlyInAnyOrder(meterId1, meterId2);
    assertThat(this.meterRegistry.find("my.timer").tag("command", "test1").timer()).isNotNull();
    assertThat(this.meterRegistry.find("my.timer").tag("command", "test2").timer()).isNotNull();
  }

  @Test
  public void nullTenantOrMeterId() {
    Meter.Id meterId = Timer.builder("my.timer").register(this.meterRegistry).getId();

    assertThatThrownBy(() -> consumer.trackMeterId(null, meterId))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> consumer.trackMeterId("someTenant", null))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(consumer.getTenantMetrics()).isEmpty();
    // Metric registered above is still there
    assertThat(this.meterRegistry.find("my.timer").timer()).isNotNull();
  }

  @Test
  void removeTrackedMetersForTenantAndFromRegistry() {
    String tenant1 = "tenant1";
    String tenant2 = "tenant2";
    // Register metrics
    Meter.Id t1m1 = Timer.builder("t1.m1").tag("tenant", tenant1).register(meterRegistry).getId();
    Meter.Id t1m2 = Timer.builder("t1.m2").tag("tenant", tenant1).register(meterRegistry).getId();
    Meter.Id t2m1 = Timer.builder("t2.m1").tag("tenant", tenant2).register(meterRegistry).getId();

    // Track them
    consumer.trackMeterId(tenant1, t1m1);
    consumer.trackMeterId(tenant1, t1m2);
    consumer.trackMeterId(tenant2, t2m1);

    // Verify they are in the registry before removal
    assertThat(meterRegistry.find(t1m1.getName()).tags(t1m1.getTags()).timer()).isNotNull();
    assertThat(meterRegistry.find(t1m2.getName()).tags(t1m2.getTags()).timer()).isNotNull();
    assertThat(meterRegistry.find(t2m1.getName()).tags(t2m1.getTags()).timer()).isNotNull();

    // Act: Deactivate tenant1
    consumer.accept(tenant1, RemovalCause.EXPIRED);

    // Assert
    // 1. Check internal tracking map
    assertThat(consumer.getTenantMetrics().get(tenant1))
        .as("Metrics for tenant1 removed from tracking")
        .isNullOrEmpty();
    assertThat(consumer.getTenantMetrics().get(tenant2))
        .as("Metrics for tenant2 remain in tracking")
        .containsExactly(t2m1);

    // 2. Check the actual MeterRegistry
    assertThat(meterRegistry.find(t1m1.getName()).tags(t1m1.getTags()).timer())
        .as("Metric t1m1 should be removed from registry")
        .isNull();
    assertThat(meterRegistry.find(t1m2.getName()).tags(t1m2.getTags()).timer())
        .as("Metric t1m2 should be removed from registry")
        .isNull();
    assertThat(meterRegistry.find(t2m1.getName()).tags(t2m1.getTags()).timer())
        .as("Metric t2m1 for tenant2 should still be in registry")
        .isNotNull();
  }

  @Test
  void noTrackedMeters() {
    // Register some other metric not related to any tenant being deactivated
    Timer.builder("other.metric").register(meterRegistry);

    assertThatCode(() -> consumer.accept("nonExistentTenant", RemovalCause.SIZE))
        .doesNotThrowAnyException();

    // Ensure the unrelated metric is still there
    assertThat(meterRegistry.find("other.metric").timer()).isNotNull();
    assertThat(consumer.getTenantMetrics()).isEmpty();
  }

  @Test
  void trackedMeterNotFoundInRegistry() {
    String tenantId = "tenant-ghost-metric";
    // Create a Meter.Id for a metric that we will NOT register
    Meter.Id ghostMeterId =
        new Meter.Id("ghost.timer", Tags.of("tenant", tenantId), null, null, Meter.Type.TIMER);

    consumer.trackMeterId(tenantId, ghostMeterId);

    // Act: Deactivate tenant
    // This should not throw an exception even if meterRegistry.remove(ghostMeterId) returns null
    assertThatCode(() -> consumer.accept(tenantId, RemovalCause.EXPIRED))
        .doesNotThrowAnyException();

    assertThat(consumer.getTenantMetrics().get(tenantId)).isNullOrEmpty();
    // And nothing was actually removed from the (empty for this name) registry
    assertThat(meterRegistry.find("ghost.timer").timer()).isNull();
  }
}
