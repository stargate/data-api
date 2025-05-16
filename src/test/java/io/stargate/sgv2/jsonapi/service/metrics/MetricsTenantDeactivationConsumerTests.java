package io.stargate.sgv2.jsonapi.service.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.metrics.MetricsTenantDeactivationConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricsTenantDeactivationConsumerTests {
  MeterRegistry meterRegistryMock; // Mock the registry for verify remove
  MetricsTenantDeactivationConsumer consumer;

  @BeforeEach
  void setUp() {
    meterRegistryMock = mock(MeterRegistry.class);
    consumer = new MetricsTenantDeactivationConsumer(meterRegistryMock);
  }

  @Test
  void trackMeterId_shouldStoreMeterIdForTenant() {
    String tenantId = "tenant1";
    Meter.Id meterId1 =
        Timer.builder("my.timer")
            .tag("type", "a")
            .tag("tenant", tenantId)
            .register(new SimpleMeterRegistry())
            .getId();
    Meter.Id meterId2 =
        Timer.builder("my.timer")
            .tag("type", "b")
            .tag("tenant", tenantId)
            .register(new SimpleMeterRegistry())
            .getId();

    consumer.trackMeterId(tenantId, meterId1);
    consumer.trackMeterId(tenantId, meterId2);

    assertThat(consumer.getTenantMetrics().get(tenantId))
        .containsExactlyInAnyOrder(meterId1, meterId2);
  }

  @Test
  void accept_shouldRemoveTrackedMetersForTenantAndFromRegistry() {
    String tenant1 = "tenant1";
    String tenant2 = "tenant2";
    Meter.Id t1m1 =
        Timer.builder("t1.m1").tag("tenant", tenant1).register(new SimpleMeterRegistry()).getId();
    Meter.Id t1m2 =
        Timer.builder("t1.m2").tag("tenant", tenant1).register(new SimpleMeterRegistry()).getId();
    Meter.Id t2m1 =
        Timer.builder("t2.m1").tag("tenant", tenant2).register(new SimpleMeterRegistry()).getId();

    consumer.trackMeterId(tenant1, t1m1);
    consumer.trackMeterId(tenant1, t1m2);
    consumer.trackMeterId(tenant2, t2m1);

    // Stub meterRegistry.remove to return the meter if found, null otherwise
    when(meterRegistryMock.remove(any(Meter.Id.class)))
        .thenAnswer(
            invocation -> {
              Meter.Id id = invocation.getArgument(0);
              // Simulate finding and removing for these specific IDs
              if (id.equals(t1m1) || id.equals(t1m2)) {
                // Return a dummy Meter to indicate success for these IDs
                return mock(Meter.class);
              }
              return null;
            });

    // Act: Deactivate tenant1
    consumer.accept(tenant1, RemovalCause.EXPIRED);

    // Assert
    assertThat(consumer.getTenantMetrics().get(tenant1))
        .isNullOrEmpty(); // Metrics for tenant1 removed from tracking
    assertThat(consumer.getTenantMetrics().get(tenant2))
        .containsExactly(t2m1); // Metrics for tenant2 remain

    verify(meterRegistryMock).remove(t1m1);
    verify(meterRegistryMock).remove(t1m2);
    verify(meterRegistryMock, never()).remove(t2m1); // Ensure tenant2's metric wasn't removed
  }
}
