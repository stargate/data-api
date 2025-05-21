package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.SESSION_TAG;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
  void removeFromEmptyMeterRegistry() {
    assertThat(meterRegistry.getMeters()).isEmpty();

    assertThatCode(() -> consumer.accept("any-tenant", RemovalCause.EXPIRED))
        .doesNotThrowAnyException();
  }

  @Test
  void testTenantMetricRemovalLifecycle() {
    String tenant1 = "tenant1";
    String tenant2 = "tenant2";
    String tenant3 = "tenant3";

    // Create metrics for tenant1 (all are TENANT_TAG) and make sure they are registered
    meterRegistry.counter("metrics1", TENANT_TAG, tenant1, "command", "find").increment();
    meterRegistry.summary("metrics1", TENANT_TAG, tenant1, "command", "create").record(10);
    meterRegistry
        .timer("metrics2", TENANT_TAG, tenant1, "command", "insert")
        .record(java.time.Duration.ofMillis(10));
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(TENANT_TAG, tenant1).timer()).isNotNull();

    // Create metrics for tenant2(all are SESSION_TAG) and make sure they are registered
    meterRegistry.counter("metrics1", SESSION_TAG, tenant2, "command", "find").increment();
    meterRegistry.summary("metrics1", SESSION_TAG, tenant2, "command", "create").record(20);
    meterRegistry
        .timer("metrics2", SESSION_TAG, tenant2, "command", "insert")
        .record(java.time.Duration.ofMillis(20));
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant2).timer()).isNotNull();

    // Create metrics for tenant3 (mix SESSION_TAG and TENANT_TAG) and make sure they are registered
    meterRegistry.counter("metrics1", TENANT_TAG, tenant3, "command", "find").increment();
    meterRegistry.summary("metrics1", TENANT_TAG, tenant3, "command", "create").record(30);
    meterRegistry
        .timer("metrics2", SESSION_TAG, tenant3, "command", "insert")
        .record(java.time.Duration.ofMillis(30));
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant3).timer()).isNotNull();

    // Case 1: Remove `null` tenantId and it should not remove anything
    assertThatCode(() -> consumer.accept(null, RemovalCause.SIZE)).doesNotThrowAnyException();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(TENANT_TAG, tenant1).timer()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant2).timer()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant3).timer()).isNotNull();

    // Case 2: Remove a non-existent tenantId and it should not remove anything
    assertThatCode(() -> consumer.accept("nonExistentTenant", RemovalCause.SIZE))
        .doesNotThrowAnyException();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(TENANT_TAG, tenant1).timer()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant2).timer()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant3).timer()).isNotNull();

    // Case 3: Remove tenant1 and it should remove all metrics for tenant1 and remain all metrics
    // for tenant2
    assertThatCode(() -> consumer.accept(tenant1, RemovalCause.EXPIRED)).doesNotThrowAnyException();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).counter()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).summary()).isNull();
    assertThat(meterRegistry.find("metrics2").tags(TENANT_TAG, tenant1).timer()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant2).timer()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant3).timer()).isNotNull();

    // Case 4: Remove tenant1 again and it should not remove anything
    assertThatCode(() -> consumer.accept(tenant1, RemovalCause.REPLACED))
        .doesNotThrowAnyException();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).counter()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).summary()).isNull();
    assertThat(meterRegistry.find("metrics2").tags(TENANT_TAG, tenant1).timer()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant2).timer()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant3).timer()).isNotNull();

    // Case 5: Remove tenant2 and it should remove all metrics for tenant2 and only tenant3 metrics
    // remain
    assertThatCode(() -> consumer.accept(tenant2, RemovalCause.EXPIRED)).doesNotThrowAnyException();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).counter()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant1).summary()).isNull();
    assertThat(meterRegistry.find("metrics2").tags(TENANT_TAG, tenant1).timer()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).counter()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(SESSION_TAG, tenant2).summary()).isNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant2).timer()).isNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).counter()).isNotNull();
    assertThat(meterRegistry.find("metrics1").tags(TENANT_TAG, tenant3).summary()).isNotNull();
    assertThat(meterRegistry.find("metrics2").tags(SESSION_TAG, tenant3).timer()).isNotNull();

    // Case 6: Remove tenant3 and it should remove all metrics for tenant3 and nothing remain
    assertThatCode(() -> consumer.accept(tenant3, RemovalCause.EXPLICIT))
        .doesNotThrowAnyException();
    assertThat(meterRegistry.getMeters()).isEmpty();
  }
}
