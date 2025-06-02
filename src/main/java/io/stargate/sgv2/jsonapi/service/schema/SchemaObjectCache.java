package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionFactory;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.util.DynamicTTLCache;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaObjectCache
    extends DynamicTTLCache<SchemaObjectCache.SchemaCacheKey, SchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaObjectCache.class);

  private final DynamicTTLSupplier ttlSupplier;

  SchemaObjectCache(
      long cacheMaxSize,
      Duration cacheTTL,
      UserAgent slaUserAgent,
      Duration slaUserTTL,
      SchemaObjectFactory schemaObjectFactory,
      MeterRegistry meterRegistry) {
    this(
        cacheMaxSize,
        cacheTTL,
        slaUserAgent,
        slaUserTTL,
        schemaObjectFactory,
        meterRegistry,
        false,
        null);
  }

  @VisibleForTesting
  SchemaObjectCache(
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
        createOnLoad(schemaObjectFactory),
        List.of(),
        meterRegistry,
        asyncTaskOnCaller,
        cacheTicker);

    Objects.requireNonNull(schemaObjectFactory, "schemaObjectFactory must not be null");
    this.ttlSupplier = new DynamicTTLSupplier(cacheTTL, slaUserAgent, slaUserTTL);

    LOGGER.info(
        "Initializing SchemaObjectCache with cacheMaxSize={}, ttlSupplier={}",
        cacheMaxSize,
        ttlSupplier);
  }

  private static ValueFactory<SchemaObjectCache.SchemaCacheKey, SchemaObject> createOnLoad(
      SchemaObjectFactory factory) {

    return (cacheKey) -> {
      var requestContext =
          cacheKey
              .requestContext()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "SchemaCacheKey.onLoad - requestContext is null, weak reference was cleared"));

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "SchemaCacheKey.onLoad - loading schema object with identifier: {}, forceRefresh: {}, userAgent: {}",
            cacheKey.schemaIdentifier(),
            cacheKey.forceRefresh(),
            cacheKey.userAgent());
      }

      return factory.apply(requestContext, cacheKey.schemaIdentifier(), cacheKey.forceRefresh());
    };
  }

  /**
   * Gets a listener to use with the {@link CqlSessionFactory} to remove the schema cache entries
   * when the DB sends schema change events.
   */
  public SchemaChangeListener getSchemaChangeListener() {
    return new SchemaObjectCache.SchemaCacheSchemaChangeListener(this);
  }

  public Uni<DatabaseSchemaObject> getDatabase(
      RequestContext requestContext,
      SchemaObjectIdentifier identifier,
      UserAgent userAgent,
      boolean forceRefresh) {
    return get(requestContext, identifier, userAgent, forceRefresh);
  }

  // get keyspace
  public Uni<KeyspaceSchemaObject> getKeyspace(
      RequestContext requestContext,
      SchemaObjectIdentifier identifier,
      UserAgent userAgent,
      boolean forceRefresh) {

    return get(requestContext, identifier, userAgent, forceRefresh);
  }

  public Uni<? extends TableBasedSchemaObject> getTableBased(
      RequestContext requestContext,
      UnscopedSchemaObjectIdentifier name,
      UserAgent userAgent,
      boolean forceRefresh) {

    var collectionKey =
        createCacheKey(
            requestContext,
            SchemaObjectIdentifier.forCollection(
                requestContext.tenant(), name.keyspace(), name.objectName()),
            userAgent,
            forceRefresh);

    var tableKey =
        createCacheKey(
            requestContext,
            SchemaObjectIdentifier.forTable(
                requestContext.tenant(), name.keyspace(), name.objectName()),
            userAgent,
            false);

    // we do not know if this is a collection or a table until we load it, and we
    // cannot change the key once it is loaded, so we need to check both

    // As an optimization, we getifPresent incase the object is a table - we would get a cache miss
    // for
    // the collection key, then try to load, then discover it is a table, fail the load, then get
    // again
    // and potentially cache hit for the table.

    if (!forceRefresh) {

      var existingCollection = getIfPresent(collectionKey);
      if (existingCollection.isPresent()) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "getTableBased() - found existing collection schema object collectionKey: {}",
              collectionKey);
          return Uni.createFrom().item((TableBasedSchemaObject) existingCollection.get());
        }
      }

      var existingTable = getIfPresent(tableKey);
      if (existingTable.isPresent()) {
        // we have a cache hit for the table, return it
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "getTableBased() - found existing table schema object tableKey: {}", tableKey);
        }
        return Uni.createFrom().item((TableBasedSchemaObject) existingTable.get());
      }
    }

    // we do not have a cache hit, or we wanted to force refresh, so we need to load it
    // force refresh on the collection cache key will cause the cache to load and replace the entry
    // and if we will refresh the driver metadata. So no need to also do that on the table key.
    return get(collectionKey)
        .onFailure(
            io.stargate.sgv2.jsonapi.service.schema.SchemaObjectFactory
                .SchemaObjectTypeMismatchException.class)
        .recoverWithUni(
            () -> {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "getTableBased() - collection key load resulted in SchemaObjectTypeMismatchException, retrying collectionKey.identifier: {}, tableKey.identifier: {}",
                    collectionKey.schemaIdentifier,
                    tableKey.schemaIdentifier);
              }
              return get(tableKey);
            })
        .invoke(
            schemaObject -> {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "getTableBased() - loaded schema object with identifier: {}, collectionKey.identifier: {}, tableKey.identifier: {}",
                    schemaObject.identifier(),
                    collectionKey.schemaIdentifier,
                    tableKey.schemaIdentifier);
              }
            })
        .map(obj -> (TableBasedSchemaObject) obj);
  }

  private <T extends SchemaObject> Uni<T> get(
      RequestContext requestContext,
      SchemaObjectIdentifier identifier,
      UserAgent userAgent,
      boolean forceRefresh) {

    // todo, check and cast somehow
    return (Uni<T>) get(createCacheKey(requestContext, identifier, userAgent, forceRefresh));
  }

  @VisibleForTesting
  <T extends SchemaObject> Optional<T> getIfPresent(
      RequestContext requestContext, SchemaObjectIdentifier identifier, UserAgent userAgent) {

    return (Optional<T>) getIfPresent(createCacheKey(requestContext, identifier, userAgent, false));
  }

  //
  //  private <T extends SchemaObject> Optional<T> getIfPresent(SchemaCacheKey key) {
  //
  //    // todo, check and cast somehow
  //    return (Optional<T>) getIfPresent(key);
  //  }
  protected void evictTable(Tenant tenant, CqlIdentifier keyspace, CqlIdentifier table) {
    // no need to normalize the keyspace name, it is already normalized
    var evictKey =
        createCacheKey(null, SchemaObjectIdentifier.forTable(tenant, keyspace, table), null, false);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("evictTable() - evictKey: {}", evictKey);
    }

    evict(evictKey);
  }

  protected void evictKeyspace(Tenant tenant, CqlIdentifier keyspace, boolean evictAll) {
    // no need to normalize the keyspace name, it is already normalized
    var evictKey =
        createCacheKey(null, SchemaObjectIdentifier.forKeyspace(tenant, keyspace), null, false);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("evictKeyspace() - evictKey: {}", evictKey);
    }

    evict(evictKey);
    if (evictAll) {
      // we need to remove all the tables, collections, etc that are in this keyspace
      evictIf(key -> evictKey.schemaIdentifier.isSameKeyspace(key.schemaIdentifier));
    }
  }

  private SchemaCacheKey createCacheKey(
      RequestContext requestContext,
      SchemaObjectIdentifier schemaIdentifier,
      UserAgent userAgent,
      boolean forceRefresh) {

    // sanity check
    // requestContext may be null when we are doing evictions
    if (requestContext!= null && (!requestContext.tenant().equals(schemaIdentifier.tenant()))){
      throw new IllegalArgumentException(
          "RequestContext tenant does not match schemaIdentifier requestContext.tenant: "
              + requestContext.tenant()
              + ", schemaIdentifier.tenant: "
              + schemaIdentifier.tenant());
    }

    return new SchemaCacheKey(
        requestContext,
        schemaIdentifier,
        ttlSupplier.ttlForUsageAgent(userAgent),
        forceRefresh,
        userAgent);
  }

  static class SchemaCacheKey implements DynamicTTLCache.CacheKey {

    private final SchemaObjectIdentifier schemaIdentifier;
    // ttl is not part of the key identity
    private final Duration ttl;
    private final boolean forceRefresh;
    // user agent only added for logging and debugging, not part of the key identity
    private final UserAgent userAgent;
    // held as weak because a cache key can be very long-lived, this is the context we loaded it in
    private WeakReference<RequestContext> requestContextWeakReference;

    SchemaCacheKey(
        RequestContext requestContext,
        SchemaObjectIdentifier schemaIdentifier,
        Duration ttl,
        boolean forceRefresh,
        UserAgent userAgent) {

      this.schemaIdentifier =
          Objects.requireNonNull(schemaIdentifier, "schemaIdentifier must not be null");
      this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
      this.forceRefresh = forceRefresh;
      this.userAgent = userAgent;
      // requestContext may be null, it is only used when we will need to load the schema object
      this.requestContextWeakReference = new WeakReference<>(requestContext);
    }

    SchemaObjectIdentifier schemaIdentifier() {
      return schemaIdentifier;
    }

    Optional<RequestContext> requestContext() {
      return Optional.ofNullable(requestContextWeakReference.get());
    }

    UserAgent userAgent() {
      return userAgent;
    }

    @Override
    public Duration ttl() {
      return ttl;
    }

    @Override
    public boolean forceRefresh() {
      return forceRefresh;
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
        schemaObjectCache.evictTable(tenant, tableMetadata.getKeyspace(), tableMetadata.getName());
      }
    }

    private void evictKeyspace(
        String context, KeyspaceMetadata keyspaceMetadata, boolean evictAll) {
      if (isSessionReady(context)) {
        schemaObjectCache.evictKeyspace(tenant, keyspaceMetadata.getName(), evictAll);
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

    /** When keyspace created, evict KS and all other objects incase we missed the drop */
    @Override
    public void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspace) {
      evictKeyspace("onKeyspaceCreated", keyspace, true);
    }

    /** When keyspace updated, evict from cache incase stale keyspace or collections */
    @Override
    public void onKeyspaceUpdated(
        @NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
      evictKeyspace("onKeyspaceUpdated", current, false);
    }
  }

  /** Called to create a new session when one is needed. */
  interface SchemaObjectFactory {
    CompletionStage<SchemaObject> apply(
        RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh);
  }
}
