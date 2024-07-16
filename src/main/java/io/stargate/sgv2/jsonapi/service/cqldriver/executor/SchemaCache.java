package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.CASSANDRA;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

/** Caches the vector enabled status for all the namespace in schema */
@ApplicationScoped
public class SchemaCache {

  @Inject QueryExecutor queryExecutor;

  @Inject ObjectMapper objectMapper;

  @Inject OperationsConfig operationsConfig;

  private final Cache<CacheKey, NamespaceCache> schemaCache =
      Caffeine.newBuilder().maximumSize(1000).build();

  public Uni<CollectionSchemaObject> getCollectionSettings(
      DataApiRequestInfo dataApiRequestInfo,
      Optional<String> tenant,
      String namespace,
      String collectionName) {
    if (CASSANDRA.equals(operationsConfig.databaseConfig().type())) {
      // default_tenant is for oss run
      final NamespaceCache namespaceCache =
          schemaCache.get(
              new CacheKey(Optional.of(tenant.orElse("default_tenant")), namespace),
              this::addNamespaceCache);
      return namespaceCache.getCollectionProperties(dataApiRequestInfo, collectionName);
    }
    final NamespaceCache namespaceCache =
        schemaCache.get(new CacheKey(tenant, namespace), this::addNamespaceCache);
    return namespaceCache.getCollectionProperties(dataApiRequestInfo, collectionName);
  }

  /** Evict collectionSetting Cache entry when there is a drop table event */
  public void evictCollectionSettingCacheEntry(
      Optional<String> tenant, String namespace, String collectionName) {
    final NamespaceCache namespaceCache = schemaCache.getIfPresent(new CacheKey(tenant, namespace));
    if (namespaceCache != null) {
      namespaceCache.evictCollectionSettingCacheEntry(collectionName);
    }
  }

  private NamespaceCache addNamespaceCache(CacheKey cacheKey) {
    return new NamespaceCache(cacheKey.namespace(), queryExecutor, objectMapper);
  }

  /**
   * When a sessionCache entry expires, evict all corresponding entire NamespaceCaches for the
   * tenant This is to ensure there is no offset for sessionCache and schemaCache
   */
  public void evictNamespaceCacheEntriesForTenant(
      String tenant, Map<CqlIdentifier, KeyspaceMetadata> keyspaces) {
    for (Map.Entry<CqlIdentifier, KeyspaceMetadata> cqlIdentifierKeyspaceMetadataEntry :
        keyspaces.entrySet()) {
      schemaCache.invalidate(
          new CacheKey(
              Optional.ofNullable(tenant),
              cqlIdentifierKeyspaceMetadataEntry.getKey().asInternal()));
    }
  }

  /** Evict corresponding namespaceCache When there is a keyspace drop event */
  public void evictNamespaceCacheEntriesForTenant(String tenant, String keyspace) {
    schemaCache.invalidate(new CacheKey(Optional.ofNullable(tenant), keyspace));
  }

  record CacheKey(Optional<String> tenant, String namespace) {}
}
