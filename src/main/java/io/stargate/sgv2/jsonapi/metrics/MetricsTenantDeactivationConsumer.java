package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.SESSION_TAG;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CQLSessionCache.DeactivatedTenantConsumer} responsible for removing tenant-specific
 * metrics from the {@link MeterRegistry} when a tenant's session is evicted from the {@link
 * CQLSessionCache}.
 *
 * <p>This consumer maintains an internal map to track {@link Meter.Id}s associated with each
 * tenant. The {@link MeteredCommandProcessor} (or other metric-producing components) should call
 * {@link #trackMeterId(String, Meter.Id)} to register tenant-specific meters with this consumer.
 *
 * <p>When a tenant's session is deactivated (e.g., due to TTL expiration or cache size limits in
 * {@code CQLSessionCache}), the {@link #accept(String, RemovalCause)} method is invoked. This
 * method then removes all tracked {@code Meter.Id}s for that tenant from its internal map and,
 * crucially, attempts to remove them from the {@code MeterRegistry}, thus stopping the reporting of
 * stale metrics.
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
   * Called by {@link CQLSessionCache} when a tenant's session is removed. This method removes all
   * tracked metrics for the specified tenant from the internal tracking map and attempts to remove
   * them from the {@link MeterRegistry}.
   *
   * @param tenantId The ID of the tenant whose session was deactivated.
   * @param cause The reason for the removal from the cache.
   */
  @Override
  public void accept(String tenantId, RemovalCause cause) {
    if (tenantId == null) {
      LOGGER.info("Received null tenantId for deactivation");
      return;
    }

    // iterate all the metrics in the meterRegistry
    for (Meter meter : meterRegistry.getMeters()) {
      String tenantTagValue = meter.getId().getTag(TENANT_TAG);
      String sessionTagValue = meter.getId().getTag(SESSION_TAG);
      if (Objects.equals(tenantTagValue, tenantId) || Objects.equals(sessionTagValue, tenantId)) {
        meterRegistry.remove(meter.getId());
      }
    }
  }
}
