package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.github.benmanes.caffeine.cache.Ticker;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.DynamicTTLCache;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaObjectCache
    extends DynamicTTLCache<SchemaObjectCache.SchemaCacheKey, SchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaObjectCache.class);

  private final DynamicTTLSupplier ttlSupplier;
  private final SchemaObjectFactory schemaObjectFactory;

  SchemaObjectCache(
      DatabaseType databaseType,
      long cacheMaxSize,
      Duration cacheTTL,
      UserAgent slaUserAgent,
      Duration slaUserTTL,
      SchemaObjectFactory schemaObjectFactory,
      MeterRegistry meterRegistry,
      boolean asyncTaskOnCaller,
      Ticker cacheTicker) {
    super(
        "schema_object_cache",
        cacheMaxSize,
        cacheKey -> schemaObjectFactory.apply(cacheKey.schemaIdentifier()),
        List.of(),
        meterRegistry,
        asyncTaskOnCaller,
        cacheTicker);

    this.schemaObjectFactory =
        Objects.requireNonNull(schemaObjectFactory, "schemaObjectFactory must not be null");
    this.ttlSupplier = new DynamicTTLSupplier(cacheTTL, slaUserAgent, slaUserTTL);

    LOGGER.info(
        "Initializing SchemaObjectCache with databaseType={}, cacheMaxSize={}, ttlSupplier={}",
        databaseType,
        cacheMaxSize,
        ttlSupplier);
  }

  public CollectionSchemaObject getCollection(
      RequestContext requestContext, String keyspace, String collection, boolean forceRefresh) {
    return get(
        SchemaObjectIdentifier.forCollection(requestContext.getTenant(), keyspace, collection),
        requestContext.getUserAgent(),
        forceRefresh);
  }

  public TableSchemaObject getTable(
      RequestContext requestContext, String keyspace, String table, boolean forceRefresh) {
    return get(
        SchemaObjectIdentifier.forTable(requestContext.getTenant(), keyspace, table),
        requestContext.getUserAgent(),
        forceRefresh);
  }

  private <T extends SchemaObject> T get(
      SchemaObjectIdentifier identifier,
      UserAgent userAgent,
      boolean forceRefresh) {
    // todo, check and cast somehow
    return (T) get(createCacheKey(identifier, userAgent), forceRefresh);
  }

  protected void evictTable(Tenant tenant, String keyspace, String table) {
    // no need to normalize the keyspace name, it is already normalized
    var evictKey = createCacheKey(SchemaObjectIdentifier.forTable(tenant, keyspace, table), null);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("evictTable() - evictKey: {}", evictKey);
    }

    evict(evictKey);
  }

  protected void evictKeyspace(Tenant tenant, String keyspace, boolean evictAll) {
    // no need to normalize the keyspace name, it is already normalized
    var evictKey = createCacheKey(SchemaObjectIdentifier.forKeyspace(tenant, keyspace), null);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("evictKeyspace() - evictKey: {}", evictKey);
    }

    evict(evictKey);
    if (evictAll) {
      // we need to remove all the tables, collections, etc that are in this keyspace
      evictIf(evictKey::isSameKeyspace);
    }
  }

  private SchemaCacheKey createCacheKey(
      SchemaObjectIdentifier schemaIdentifier,
      UserAgent userAgent) {

    return new SchemaCacheKey(
        schemaIdentifier,
        ttlSupplier.ttlForUsageAgent(userAgent),
        userAgent);
  }

  static class SchemaCacheKey implements DynamicTTLCache.CacheKey {

    private final SchemaObjectIdentifier schemaIdentifier;
    // ttl is not part of the key identity
    private final Duration ttl;
    // user agent only added for logging and debugging, not part of the key identity
    private final UserAgent userAgent;

    SchemaCacheKey(
        SchemaObjectIdentifier schemaIdentifier,
        Duration ttl,
        UserAgent userAgent) {

      this.schemaIdentifier =
          Objects.requireNonNull(schemaIdentifier, "schemaIdentifier must not be null");
      this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
      this.userAgent = userAgent;
    }

    SchemaObjectIdentifier schemaIdentifier() {
      return schemaIdentifier;
    }

    @Override
    public Duration ttl() {
      return ttl;
    }

    boolean isSameKeyspace(SchemaCacheKey other) {
      return schemaIdentifier.isSameKeyspace(other.schemaIdentifier);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SchemaCacheKey that)) {
        return false;
      }
      return schemaIdentifier.equals(that.schemaIdentifier);
    }

    @Override
    public int hashCode() {
      return Objects.hash(schemaIdentifier);
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("SchemaCacheKey{")
          .append("schemaIdentifier=")
          .append(schemaIdentifier)
          .append(", ttl=")
          .append(ttl)
          .append(", userAgent='")
          .append(userAgent)
          .append("'}")
          .toString();
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
   * <p>If the tenant is not set, null or blank, we log at ERROR rather than throw because the
   * callback methods are called on driver async threads and exceptions there are unlikely to be
   * passed back in the request response.
   *
   * <p>This could be non-static inner, but static to make testing easier so we can pass in the
   * cache it is working with.
   */
  static class SchemaCacheSchemaChangeListener extends SchemaChangeListenerBase {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(SchemaCacheSchemaChangeListener.class);

    private final SchemaObjectCache schemaObjectCache;

    // tenantID is nullable when there is a regular C* database
    private boolean sessionReady = false;
    private Tenant tenant = null;

    public SchemaCacheSchemaChangeListener(SchemaObjectCache schemaObjectCache) {
      this.schemaObjectCache =
          Objects.requireNonNull(schemaObjectCache, "schemaObjectCache must not be null");
    }

    private boolean isSessionReady(String context) {
      if (!sessionReady) {
        LOGGER.error(
            "SchemaCacheSchemaChangeListener sessionReady is false when expected true - {}",
            context);
        return false;
      }
      return true;
    }

    private void evictTable(String context, TableMetadata tableMetadata) {
      if (isSessionReady(context)) {
        schemaObjectCache.evictTable(
            tenant,
            tableMetadata.getKeyspace().asInternal(),
            tableMetadata.getName().asInternal());
      }
    }

    private void evictKeyspace(
        String context, KeyspaceMetadata keyspaceMetadata, boolean evictAll) {
      if (isSessionReady(context)) {
        schemaObjectCache.evictKeyspace(
            tenant, keyspaceMetadata.getName().asInternal(), evictAll);
      }
    }

    @Override
    public void onSessionReady(@NonNull Session session) {
      // This is called when the session is ready, we can get the tenant from the session name
      // and set it in the listener so we can use it in the other methods.
      tenant = TenantFactory.instance().create(session.getName());
      sessionReady = true;
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
      evictKeyspace("onKeyspaceDropped", keyspace, true);
    }

    /** When keyspace created, evict from cache incase stale keyspace or collections */
    @Override
    public void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspace) {
      evictKeyspace("onKeyspaceCreated", keyspace, false);
    }

    /** When keyspace updated, evict from cache incase stale keyspace or collections */
    @Override
    public void onKeyspaceUpdated(
        @NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
      evictKeyspace("onKeyspaceUpdated", current, false);
    }
  }

  /** Called to create a new session when one is needed. */
  @FunctionalInterface
  interface SchemaObjectFactory
      extends Function<SchemaObjectIdentifier, CompletionStage<SchemaObject>> {
    CompletionStage<SchemaObject> apply(SchemaObjectIdentifier identifier);
  }
}
