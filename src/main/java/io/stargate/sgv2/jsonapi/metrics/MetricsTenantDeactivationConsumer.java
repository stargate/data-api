package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.SESSION_TAG;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CQLSessionCache.DeactivatedTenantConsumer} responsible for removing tenant-specific
 * metrics from the {@link MeterRegistry} when a tenant's session is evicted from the {@link
 * CQLSessionCache}.
 */
public class MetricsTenantDeactivationConsumer
    implements CQLSessionCache.DeactivatedTenantConsumer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MetricsTenantDeactivationConsumer.class);
  private final MeterRegistry meterRegistry;

  public MetricsTenantDeactivationConsumer(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  /**
   * Called by {@link CQLSessionCache} when a tenant's session is removed. This method iterates
   * through all registered meters in the {@link MeterRegistry} and removes any that are tagged with
   * the specified {@code tenantId} using either the {@link MetricsConstants.MetricTags#TENANT_TAG}
   * or {@link MetricsConstants.MetricTags#SESSION_TAG} key.
   *
   * @param tenantId The ID of the tenant whose session was deactivated. This value will be used to
   *     find metrics with a matching tag.
   * @param cause The reason for the removal from the cache.
   */
  @Override
  public void accept(String tenantId, RemovalCause cause) {
    if (tenantId == null) {
      LOGGER.warn("Received null tenantId for deactivation");
      return;
    }

    for (Meter meter : meterRegistry.getMeters()) {
      String tenantTagValue = meter.getId().getTag(TENANT_TAG);
      String sessionTagValue = meter.getId().getTag(SESSION_TAG);
      if (Objects.equals(tenantTagValue, tenantId) || Objects.equals(sessionTagValue, tenantId)) {
        Meter removedMeter = meterRegistry.remove(meter.getId());
        if (removedMeter == null) {
          LOGGER.warn(
              "Attempted to remove metric with ID {} for tenant {} but it was not found in the registry during the removal phase.",
              meter.getId(),
              tenantId);
        }
      }
    }
  }
}
