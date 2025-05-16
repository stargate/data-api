package io.stargate.sgv2.jsonapi.metrics;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsTenantDeactivationConsumer
    implements CQLSessionCache.DeactivatedTenantConsumer {
  private final MeterRegistry meterRegistry;
  private final Map<String, List<Meter.Id>> tenantMetrics = new ConcurrentHashMap<>();

  public MetricsTenantDeactivationConsumer(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public void trackMeterId(String tenantId, Meter.Id meterId) {
    tenantMetrics
        .computeIfAbsent(tenantId, k -> Collections.synchronizedList(new ArrayList<>()))
        .add(meterId);
  }

  // For testing purposes
  public Map<String, List<Meter.Id>> getTenantMetrics() {
    return Collections.unmodifiableMap(tenantMetrics);
  }

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
