package io.stargate.sgv2.jsonapi.metrics;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
@ApplicationScoped
public class MetricsTenantDeactivationConsumer
    implements CQLSessionCache.DeactivatedTenantConsumer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MetricsTenantDeactivationConsumer.class);
  private final MeterRegistry meterRegistry;

  private final Map<String, List<Meter.Id>> tenantMetrics = new ConcurrentHashMap<>();

  @Inject
  public MetricsTenantDeactivationConsumer(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  /**
   * Tracks a specific {@link Meter.Id} for a given tenant. This method should be called by
   * components that create tenant-specific metrics that need to be cleaned up when the tenant
   * becomes inactive.
   *
   * @param tenantId The identifier of the tenant.
   * @param meterId The {@link Meter.Id} of the metric to track.
   * @throws IllegalArgumentException if {@code tenantId} or {@code meterId} is null.
   */
  public void trackMeterId(String tenantId, Meter.Id meterId) {
    if (tenantId == null || meterId == null) {
      LOGGER.error(
          "Attempted to track meter with null tenantId or meterId. TenantId: {}, MeterId: {}",
          tenantId,
          meterId);
      // TODO: should we throw an exception here?
      throw new IllegalArgumentException("Tenant ID and Meter ID must not be null");
    }
    tenantMetrics
        .computeIfAbsent(tenantId, k -> Collections.synchronizedList(new ArrayList<>()))
        .add(meterId);
  }

  /**
   * Returns an unmodifiable view of the currently tracked tenant metrics. Intended primarily for
   * testing purposes.
   */
  public Map<String, List<Meter.Id>> getTenantMetrics() {
    return Collections.unmodifiableMap(tenantMetrics);
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
    List<Meter.Id> meterIdsForTenant = tenantMetrics.remove(tenantId);
    if (meterIdsForTenant != null && !meterIdsForTenant.isEmpty()) {
      for (Meter.Id meterId : meterIdsForTenant) {
        meterRegistry.remove(meterId);
      }
    }
  }
}
