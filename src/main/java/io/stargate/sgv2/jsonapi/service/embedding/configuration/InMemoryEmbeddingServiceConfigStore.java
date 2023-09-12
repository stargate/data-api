package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

@LookupIfProperty(name = "stargate.jsonapi.embedding.config.store", stringValue = "in-memory")
@ApplicationScoped
public class InMemoryEmbeddingServiceConfigStore implements EmbeddingServiceConfigStore {

  private static final Cache<CacheKey, ServiceConfig> serviceConfigStore =
      Caffeine.newBuilder().maximumSize(1000).build();

  @Override
  public void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig) {
    serviceConfigStore.put(new CacheKey(tenant, serviceConfig.serviceName()), serviceConfig);
  }

  @Override
  public ServiceConfig getConfiguration(Optional<String> tenant, String serviceName) {
    return serviceConfigStore.getIfPresent(new CacheKey(tenant, serviceName));
  }

  record CacheKey(Optional<String> tenant, String namespace) {}
}
