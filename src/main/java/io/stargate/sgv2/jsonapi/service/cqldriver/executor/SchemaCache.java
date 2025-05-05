package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.DEFAULT_TENANT;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheFactory;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level entry for caching the keyspaces and tables from the backend db
 *
 * <p>IMPORTANT: use {@link #getSchemaChangeListener()} and {@link #getDeactivatedTenantConsumer()}
 * to get callbacks to evict the cache when the schema changes or a tenant is deactivated. This
 * should be handled in {@link io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheFactory}
 *
 * <p>TODO: There should be a single level cache of keyspace,table not two levels, it will be easier
 * to size and manage https://github.com/stargate/data-api/issues/2070
 */
@ApplicationScoped
public class SchemaCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCache.class);

  private final CqlSessionCacheFactory sessionCacheFactory;
  private final DatabaseType databaseType;
  private final ObjectMapper objectMapper;
  private final TableCacheFactory tableCacheFactory;
  private final OperationsConfig operationsConfig;

  /** caching the keyspaces we know about which then have all the tables / collections under them */
  private final LoadingCache<KeyspaceCacheKey, TableBasedSchemaCache> keyspaceCache;

  @Inject
  public SchemaCache(
      CqlSessionCacheFactory sessionCacheFactory,
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig) {
    this(sessionCacheFactory, objectMapper, operationsConfig, TableBasedSchemaCache::new);
  }

  /**
   * NOTE: must not use the sessionCacheFactory in the ctor or because it will create a circular
   * calls, because the sessionCacheFactory calls schema cache to get listeners
   */
  SchemaCache(
      CqlSessionCacheFactory sessionCacheFactory,
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      TableCacheFactory tableCacheFactory) {

    this.sessionCacheFactory =
        Objects.requireNonNull(sessionCacheFactory, "sessionCacheFactory must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.operationsConfig = operationsConfig;
    this.databaseType =
        Objects.requireNonNull(operationsConfig, "operationsConfig must not be null")
            .databaseConfig()
            .type();
    this.tableCacheFactory =
        Objects.requireNonNull(tableCacheFactory, "tableCacheFactory must not be null");

    // TODO: The size of the cache should be in configuration.
    int cacheSize = 1000;
    keyspaceCache = Caffeine.newBuilder().maximumSize(cacheSize).build(this::onLoad);

    LOGGER.info("SchemaCache created with max size {}", cacheSize);
  }

  /**
   * Gets a listener to use with the {@link CqlSessionFactory} to remove the schema cache entries
   * when the DB sends schema change events.
   */
  public SchemaChangeListener getSchemaChangeListener() {
    return new SchemaCacheSchemaChangeListener(this);
  }

  /**
   * Gets a consumer to use with the {@link CQLSessionCache} to remove the schema cache entries when
   * a tenant is deactivated.
   */
  public CQLSessionCache.DeactivatedTenantConsumer getDeactivatedTenantConsumer() {
    return new SchemaCacheDeactivatedTenantConsumer(this);
  }

  /** Gets or loads the schema object for the given namespace and collection or table name. */
  public Uni<SchemaObject> getSchemaObject(
      RequestContext requestContext,
      String namespace,
      String collectionName,
      boolean forceRefresh) {

    Objects.requireNonNull(namespace, "namespace must not be null");

    // Aaron 1 may 2025 - Based on existing logic I thought we should have a non null tenantId in
    // all cases other than
    // when using cassandra as a backend.
    var resolvedTenantId =
        (databaseType == DatabaseType.CASSANDRA)
            ? requestContext.getTenantId().orElse(DEFAULT_TENANT)
            : requestContext.getTenantId().orElseThrow();

    var tableBasedSchemaCache =
        keyspaceCache.get(new KeyspaceCacheKey(resolvedTenantId, namespace));
    Objects.requireNonNull(
        tableBasedSchemaCache, "keyspaceCache must not return null tableBasedSchemaCache");
    return tableBasedSchemaCache.getSchemaObject(requestContext, collectionName, forceRefresh);
  }

  private TableBasedSchemaCache onLoad(SchemaCache.KeyspaceCacheKey key) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onLoad() - tenantId: {}, keyspace: {}", key.tenantId(), key.keyspace());
    }

    // MUST not use the session cache in the ctor, OK to create a query executor for each inner
    // cache here
    var queryExecutor = new QueryExecutor(sessionCacheFactory.create(), operationsConfig);
    return tableCacheFactory.create(key.keyspace(), queryExecutor, objectMapper);
  }

  Optional<SchemaObject> peekSchemaObject(String tenantId, String keyspaceName, String tableName) {

    var tableBasedSchemaCache =
        keyspaceCache.getIfPresent(new KeyspaceCacheKey(tenantId, keyspaceName));
    if (tableBasedSchemaCache != null) {
      return tableBasedSchemaCache.peekSchemaObject(tableName);
    }
    return Optional.empty();
  }
  ;

  /** Removes the table from the cache if present. */
  void evictTable(String tenantId, String keyspace, String tableName) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "evictTable() - tenantId: {}, keyspace: {}, tableName: {}",
          tenantId,
          keyspace,
          tableName);
    }

    var tableBasedSchemaCache =
        keyspaceCache.getIfPresent(new KeyspaceCacheKey(tenantId, keyspace));
    if (tableBasedSchemaCache != null) {
      tableBasedSchemaCache.evictCollectionSettingCacheEntry(tableName);
    }
  }

  /** Removes all keyspaces and table entries for the given tenantId from the cache. */
  void evictAllKeyspaces(String tenantId) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("evictAllKeyspaces() - tenantId: {}", tenantId);
    }

    keyspaceCache.asMap().keySet().removeIf(key -> key.tenantId().equals(tenantId));
  }

  /** Removes the keyspace from the cache if present. */
  void evictKeyspace(String tenant, String keyspace) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("evictKeyspace() - tenantId: {}, keyspace: {}", tenant, keyspace);
    }

    keyspaceCache.invalidate(new KeyspaceCacheKey(tenant, keyspace));
  }

  /** Key for the Keyspace cache, we rely on the record hash and equals */
  record KeyspaceCacheKey(String tenantId, String keyspace) {

    KeyspaceCacheKey {
      Objects.requireNonNull(tenantId, "tenantId must not be null");
      Objects.requireNonNull(keyspace, "namespace must not be null");
    }
  }

  /**
   * SchemaChangeListener for the schema cache, this is used to evict the cache entries when we get
   * messages from the DB that the schema has changed.
   *
   * <p>NOTE: This relies on the sessionName being set correctly which should be in {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionFactory}
   *
   * <p>A new schema change listener should be created for each CQL {@link Session} when it is
   * created because the listener will first listen for {@link SchemaChangeListener#onSessionReady}
   * and get the tenantID from the session name via {@link Session#getName}.
   *
   * <p>If the tenantId is not set, null or blank, we log at ERROR rather than throw because the
   * callback methods are called on driver async threads and exceptions there are unlikely to be
   * passed back in the request response.
   *
   * <p>This could be non-static inner, but statis to make testing easier so we can pass in the
   * cache it is working with.
   */
  static class SchemaCacheSchemaChangeListener extends SchemaChangeListenerBase {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(SchemaCacheSchemaChangeListener.class);

    private final SchemaCache schemaCache;

    private String tenantId = null;

    public SchemaCacheSchemaChangeListener(SchemaCache schemaCache) {
      this.schemaCache = Objects.requireNonNull(schemaCache, "schemaCache must not be null");
    }

    private boolean isTenantId(String context) {
      if (tenantId == null || tenantId.isBlank()) {
        LOGGER.error(
            "SchemaCacheSchemaChangeListener tenantId is null or blank when expected to be set - {}",
            context);
        return false;
      }
      return true;
    }

    private void evictTable(String context, TableMetadata tableMetadata) {
      if (isTenantId(context)) {
        schemaCache.evictTable(
            tenantId,
            tableMetadata.getKeyspace().asInternal(),
            tableMetadata.getName().asInternal());
      }
    }

    @Override
    public void onSessionReady(@NonNull Session session) {
      // This is called when the session is ready, we can get the tenantId from the session name
      // and set it in the listener so we can use it in the other methods.
      tenantId = session.getName();
      isTenantId("onSessionReady called but sessionName() is null or blank");
    }

    /**
     * When a table is dropped, evict from cache to reduce the size and avoid stale if it is
     * re-created
     */
    @Override
    public void onTableDropped(@NonNull TableMetadata table) {
      evictTable("onTableDropped", table);
    }

    /** When a table is created, evict from cache to avoid stale if it was re-created */
    @Override
    public void onTableCreated(@NonNull TableMetadata table) {
      evictTable("onTableCreated", table);
    }

    /** When a table is updated, evict from cache to avoid stale entries */
    @Override
    public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
      // table name can never change
      evictTable("onTableUpdated", current);
    }

    /** When keyspace dropped, we dont need any more of the tables in the cache */
    @Override
    public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
      if (isTenantId("onKeyspaceDropped")) {
        schemaCache.evictKeyspace(tenantId, keyspace.getName().asInternal());
      }
    }
  }

  /**
   * Listener for use with the {@link CQLSessionCache} to remove the schema cache entries when a
   * tenant is deactivated.
   */
  private static class SchemaCacheDeactivatedTenantConsumer
      implements CQLSessionCache.DeactivatedTenantConsumer {

    private final SchemaCache schemaCache;

    public SchemaCacheDeactivatedTenantConsumer(SchemaCache schemaCache) {
      this.schemaCache = Objects.requireNonNull(schemaCache, "schemaCache must not be null");
    }

    @Override
    public void accept(String tenantId, RemovalCause cause) {
      // the sessions are keyed on the tenantID and the crednetials, and one session can work with
      // multiple keyspaces. So we need to evict all the keyspaces for the tenantId
      schemaCache.evictAllKeyspaces(tenantId);
    }
  }

  /** Function to create a new TableBasedSchemaCache, so we can mock when testing */
  @FunctionalInterface
  public interface TableCacheFactory {
    TableBasedSchemaCache create(
        String namespace, QueryExecutor queryExecutor, ObjectMapper objectMapper);
  }
}
