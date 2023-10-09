package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

/** Caches the vector enabled status for all the namespace in schema */
@ApplicationScoped
public class SchemaCache {

  @Inject private QueryExecutor queryExecutor;

  @Inject private ObjectMapper objectMapper;

  private final Cache<CacheKey, NamespaceCache> schemaCache =
      Caffeine.newBuilder().maximumSize(1000).build();

  public Uni<CollectionSettings> getCollectionProperties(
      Optional<String> tenant, String namespace, String collectionName) {
    final NamespaceCache namespaceCache =
        schemaCache.get(new CacheKey(tenant, namespace), this::addNamespaceCache);
    return namespaceCache.getCollectionProperties(collectionName);
  }

  private NamespaceCache addNamespaceCache(CacheKey cacheKey) {
    return new NamespaceCache(cacheKey.namespace(), queryExecutor, objectMapper);
  }

  record CacheKey(Optional<String> tenant, String namespace) {}
}
