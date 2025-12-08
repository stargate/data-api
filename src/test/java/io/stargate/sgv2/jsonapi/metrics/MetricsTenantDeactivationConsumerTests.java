package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.SESSION_TAG;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

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
    if (meterRegistry != null) {
      meterRegistry.clear();
      meterRegistry.close();
    }
  }

  @Test
  void removeFromEmptyMeterRegistry() {
    assertThat(meterRegistry.getMeters()).isEmpty();

    assertThatCode(() -> consumer.accept("any-tenant")).doesNotThrowAnyException();
  }

  @Test
  void testTenantMetricRemovalLifecycle() {
    final String tenant1 = "tenant1";
    final String tenant2 = "tenant2";
    final String tenant3 = "tenant3";
    final String counterMetrics = "counterMetrics";
    final String summaryMetrics = "summaryMetrics";
    final String timerMetrics = "timerMetrics";
    final String otherTag = "otherTag";
    final String otherTagValue = "otherTagValue";

    // Create metrics for tenant1 (all use TENANT_TAG) and verify they are registered
    createCounter(counterMetrics, TENANT_TAG, tenant1, otherTag, otherTagValue);
    createSummary(summaryMetrics, TENANT_TAG, tenant1, otherTag, otherTagValue);
    createTimer(timerMetrics, TENANT_TAG, tenant1, otherTag, otherTagValue);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant1);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant1);
    assertMetricExists(timerMetrics, TENANT_TAG, tenant1);

    // Create metrics for tenant2 (all use SESSION_TAG) and verify they are registered
    createCounter(counterMetrics, SESSION_TAG, tenant2, otherTag, otherTagValue);
    createSummary(summaryMetrics, SESSION_TAG, tenant2, otherTag, otherTagValue);
    createTimer(timerMetrics, SESSION_TAG, tenant2, otherTag, otherTagValue);
    assertMetricExists(counterMetrics, SESSION_TAG, tenant2);
    assertMetricExists(summaryMetrics, SESSION_TAG, tenant2);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant2);

    // Create metrics for tenant3 (mixed tags) and verify they are registered
    createCounter(counterMetrics, TENANT_TAG, tenant3, otherTag, otherTagValue);
    createSummary(summaryMetrics, TENANT_TAG, tenant3, otherTag, otherTagValue);
    createTimer(timerMetrics, SESSION_TAG, tenant3, otherTag, otherTagValue);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant3);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant3);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant3);

    // Case 1: Removing with null tenantId should not remove any metrics
    assertThatCode(() -> consumer.accept(null)).doesNotThrowAnyException();
    assertMetricExists(counterMetrics, TENANT_TAG, tenant1);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant1);
    assertMetricExists(timerMetrics, TENANT_TAG, tenant1);
    assertMetricExists(counterMetrics, SESSION_TAG, tenant2);
    assertMetricExists(summaryMetrics, SESSION_TAG, tenant2);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant2);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant3);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant3);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant3);

    // Case 2: Removing a non-existent tenantId should not remove any metrics
    assertThatCode(() -> consumer.accept("nonExistentTenant")).doesNotThrowAnyException();
    assertMetricExists(counterMetrics, TENANT_TAG, tenant1);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant1);
    assertMetricExists(timerMetrics, TENANT_TAG, tenant1);
    assertMetricExists(counterMetrics, SESSION_TAG, tenant2);
    assertMetricExists(summaryMetrics, SESSION_TAG, tenant2);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant2);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant3);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant3);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant3);

    // Case 3: Removing tenant1 should remove only tenant1's metrics; others remain
    assertThatCode(() -> consumer.accept(tenant1)).doesNotThrowAnyException();
    assertMetricNotExists(counterMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(summaryMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(timerMetrics, TENANT_TAG, tenant1);
    assertMetricExists(counterMetrics, SESSION_TAG, tenant2);
    assertMetricExists(summaryMetrics, SESSION_TAG, tenant2);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant2);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant3);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant3);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant3);

    // Case 4: Removing tenant1 again (already removed) should not affect other metrics
    assertThatCode(() -> consumer.accept(tenant1)).doesNotThrowAnyException();
    assertMetricNotExists(counterMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(summaryMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(timerMetrics, TENANT_TAG, tenant1);
    assertMetricExists(counterMetrics, SESSION_TAG, tenant2);
    assertMetricExists(summaryMetrics, SESSION_TAG, tenant2);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant2);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant3);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant3);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant3);

    // Case 5: Removing tenant2 should remove only tenant2's metrics; only tenant3's remain
    assertThatCode(() -> consumer.accept(tenant2)).doesNotThrowAnyException();
    assertMetricNotExists(counterMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(summaryMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(timerMetrics, TENANT_TAG, tenant1);
    assertMetricNotExists(counterMetrics, SESSION_TAG, tenant2);
    assertMetricNotExists(summaryMetrics, SESSION_TAG, tenant2);
    assertMetricNotExists(timerMetrics, SESSION_TAG, tenant2);
    assertMetricExists(counterMetrics, TENANT_TAG, tenant3);
    assertMetricExists(summaryMetrics, TENANT_TAG, tenant3);
    assertMetricExists(timerMetrics, SESSION_TAG, tenant3);

    // Case 6: Removing tenant3 should remove all remaining metrics (registry empty)
    assertThatCode(() -> consumer.accept(tenant3)).doesNotThrowAnyException();
    assertThat(meterRegistry.getMeters()).isEmpty();
  }

  // Helper to create a counter metric with the given name and tags
  private void createCounter(String metricName, String... tags) {
    meterRegistry.counter(metricName, Tags.of(tags)).increment();
  }

  // Helper to create a summary metric with the given name and tags
  private void createSummary(String metricName, String... tags) {
    meterRegistry.summary(metricName, Tags.of(tags)).record(10);
  }

  // Helper to create a timer metric with the given name and tags
  private void createTimer(String metricName, String... tags) {
    meterRegistry.timer(metricName, Tags.of(tags)).record(java.time.Duration.ofMillis(10));
  }

  // Helper assertions
  private void assertMetricExists(String metricName, String tagKey, String tagValue) {
    assertThat(meterRegistry.find(metricName).tag(tagKey, tagValue).meter())
        .as("Metric %s for tenant %s should exist", metricName, tagValue)
        .isNotNull();
  }

  // Helper assertions
  private void assertMetricNotExists(String metricName, String tagKey, String tagValue) {
    assertThat(meterRegistry.find(metricName).tag(tagKey, tagValue).meter())
        .as("Metric %s for tenant %s should NOT exist", metricName, tagValue)
        .isNull();
  }
}
