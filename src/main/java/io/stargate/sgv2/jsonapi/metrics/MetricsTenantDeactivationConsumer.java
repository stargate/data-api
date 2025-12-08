package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.SESSION_TAG;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CQLSessionCache.DeactivatedTenantListener} responsible for removing tenant-specific
 * metrics from the {@link MeterRegistry} when a tenant's session is evicted from the {@link
 * CQLSessionCache}.
 */
public class MetricsTenantDeactivationConsumer
    implements CQLSessionCache.DeactivatedTenantListener {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MetricsTenantDeactivationConsumer.class);
  private final MeterRegistry meterRegistry;

  public MetricsTenantDeactivationConsumer(MeterRegistry meterRegistry) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "MeterRegistry cannot be null");
  }

  /**
   * Called by {@link CQLSessionCache} when a tenant's session is removed. This method iterates
   * through all registered meters in the {@link MeterRegistry} and removes any that are tagged with
   * the specified {@code tenantId} using either the {@link MetricsConstants.MetricTags#TENANT_TAG}
   * or {@link MetricsConstants.MetricTags#SESSION_TAG} key.
   *
   * @param tenantId The ID of the tenant whose session was deactivated. This value will be used to
   *     find metrics with a matching tag.
   */
  @Override
  public void accept(String tenantId) {
    if (tenantId == null) {
      LOGGER.warn("Received null tenantId for deactivation");
      return;
    }

    for (Meter meter : meterRegistry.getMeters()) {
      // Check TENANT_TAG first, if not found, check SESSION_TAG
      if (Objects.equals(meter.getId().getTag(TENANT_TAG), tenantId)
          || Objects.equals(meter.getId().getTag(SESSION_TAG), tenantId)) {
        if (meterRegistry.remove(meter.getId()) == null) {
          LOGGER.debug(
              "Attempted to remove metric with ID {} for tenant {} but it was not found in the registry during the removal phase.",
              meter.getId(),
              tenantId);
        }
      }
    }
  }
}
