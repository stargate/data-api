package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.JsonApiStartUp;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQL session cache to reuse the session for the same tenant and token. The cache is configured to
 * expire after <code>CACHE_TTL_SECONDS</code> of inactivity and to have a maximum size of <code>
 * CACHE_TTL_SECONDS</code> sessions.
 */
@ApplicationScoped
public class CQLSessionCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);

  /** Configuration for the JSON API operations. */
  private final OperationsConfig operationsConfig;

  /**
   * Default tenant to be used when the backend is OSS cassandra and when no tenant is passed in the
   * request
   */
  private static final String DEFAULT_TENANT = "default_tenant";

  /** CQLSession cache. */
  private final LoadingCache<SessionCacheKey, CqlSession> sessionCache;

  /** SchemaCache, used for evict collectionSetting cache and namespace cache. */
  @Inject private SchemaCache schemaCache;

  /** Database type Astra */
  public static final String ASTRA = "astra";

  /** Database type OSS cassandra */
  public static final String CASSANDRA = "cassandra";

  /** Persistence type SSTable Writer */
  public static final String OFFLINE_WRITER = "offline_writer";

  @ConfigProperty(name = "quarkus.application.name")
  String APPLICATION_NAME;

  @Inject
  public CQLSessionCache(OperationsConfig operationsConfig, MeterRegistry meterRegistry) {
    LOGGER.info("Initializing CQLSessionCache");
    this.operationsConfig = operationsConfig;
    LoadingCache<SessionCacheKey, CqlSession> loadingCache =
        Caffeine.newBuilder()
            .expireAfterAccess(
                Duration.ofSeconds(operationsConfig.databaseConfig().sessionCacheTtlSeconds()))
            .maximumSize(operationsConfig.databaseConfig().sessionCacheMaxSize())
            // removal listener is invoked after the entry has been removed from the cache. So the
            // idea is that we no longer return this session for any lookup as a first step, then
            // close the session in the background asynchronously which is a graceful closing of
            // channels i.e. any in-flight query will be completed before the session is getting
            // closed.
            .removalListener(
                (RemovalListener<SessionCacheKey, CqlSession>)
                    (sessionCacheKey, session, cause) -> {
                      if (sessionCacheKey != null) {
                        if (LOGGER.isTraceEnabled()) {
                          LOGGER.trace(
                              "Removing session for tenant : {}", sessionCacheKey.tenantId());
                        }
                        if (this.schemaCache != null && session != null) {
                          // When a sessionCache entry expires
                          // Evict all corresponding entire NamespaceCaches for the tenant
                          // This is to ensure there is no offset for sessionCache and schemaCache
                          schemaCache.evictNamespaceCacheEntriesForTenant(
                              sessionCacheKey.tenantId(), session.getMetadata().getKeyspaces());
                        }
                      }
                      if (session != null) {
                        session.close();
                      }
                    })
            .recordStats()
            .build(this::getNewSession);
    this.sessionCache =
        CaffeineCacheMetrics.monitor(meterRegistry, loadingCache, "cql_sessions_cache");
    LOGGER.info(
        "CQLSessionCache initialized with ttl of {} seconds and max size of {}",
        operationsConfig.databaseConfig().sessionCacheTtlSeconds(),
        operationsConfig.databaseConfig().sessionCacheMaxSize());
  }

  /**
   * Loader for new CQLSession.
   *
   * @return CQLSession
   * @throws RuntimeException if database type is not supported
   */
  private CqlSession getNewSession(SessionCacheKey cacheKey) {

    // TODO: WHY IS THIS USED ?
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.SESSION_NAME, cacheKey.tenantId)
            .build();

    var databaseConfig = operationsConfig.databaseConfig();
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Creating new session for tenant : {} and Database type: {}",
          cacheKey.tenantId(),
          databaseConfig.type());
    }

    // there is a lot of common setup regardless of the database type
    var builder =
        new TenantAwareCqlSessionBuilder(cacheKey.tenantId())
            .withLocalDatacenter(operationsConfig.databaseConfig().localDatacenter())
            .withClassLoader(Thread.currentThread().getContextClassLoader())
            .withConfigLoader(loader)
            .addSchemaChangeListener(new SchemaChangeListener(schemaCache, cacheKey.tenantId))
            .withApplicationName(APPLICATION_NAME);
    cacheKey.credentials().addToSessionBuilder(builder);

    if (databaseConfig.type().equals(CASSANDRA)) {
      var seeds =
          Objects.requireNonNull(operationsConfig.databaseConfig().cassandraEndPoints()).stream()
              .map(
                  host ->
                      new InetSocketAddress(
                          host, operationsConfig.databaseConfig().cassandraPort()))
              .toList();
      builder.addContactPoints(seeds);
    }

    // aaron - this used to have a if / else that threw an exception if the database type was not
    // known
    // but we test that when creating the credentials for the cache key so no need to do it here.
    return builder.build();
  }

  //  /**
  //   * This method checks if the session is valid for the tenant. If a token is generated for
  // tenant A
  //   * and if it is used to access tenant B's data, the cqlsession object still gets created
  // without
  //   * any error but it has no metadata or keyspaces information. So, this situation leads to
  // return
  //   * misleading no keyspace found error, instead of authorization error.
  //   *
  //   * <p>This method checks if the session is valid, first by checking if there are any keyspaces
  // and
  //   * returns true if there are any keyspaces. If there are no keyspaces, then it tries to
  // execute a
  //   * query on system_virtual_schema.tables and returns true if the query is successful. Failure
  // to
  //   * execute the query with an UnauthorizedException means the session is invalid i.e. not meant
  // for
  //   * the tenant in the request.
  //   *
  //   * @param cqlSession CqlSession
  //   * @param tenantId tenant id
  //   * @return true if the session is valid, false otherwise
  //   */
  //  private boolean isAstraSessionValid(CqlSession cqlSession, String tenantId) {
  //    if (!cqlSession.getMetadata().getKeyspaces().isEmpty()) {
  //      return true;
  //    }
  //    try {
  //      cqlSession.execute("SELECT * FROM system_virtual_schema.tables");
  //      return true;
  //    } catch (com.datastax.oss.driver.api.core.servererrors.UnauthorizedException e) {
  //      LOGGER.error("Unauthorized to access tenant %s's data".formatted(tenantId), e);
  //      return false;
  //    }
  //  }

  /**
   * Get CQLSession from cache.
   *
   * @return CQLSession
   */
  public CqlSession getSession(DataApiRequestInfo dataApiRequestInfo) {
    // Validation happens when creating the credentials and session key
    return getSession(
        dataApiRequestInfo.getTenantId().orElse(""),
        dataApiRequestInfo.getCassandraToken().orElse(""));
  }

  public CqlSession getSession(String tenantId, String authToken) {
    String fixedToken = getFixedToken();
    if (fixedToken != null && !authToken.equals(fixedToken)) {
      throw new UnauthorizedException(ErrorCodeV1.UNAUTHENTICATED_REQUEST.getMessage());
    }

    var cacheKey = getSessionCacheKey(tenantId, authToken);
    // TODO: why is this different for OFFLINE ?
    if (OFFLINE_WRITER.equals(operationsConfig.databaseConfig().type())) {
      return sessionCache.getIfPresent(cacheKey);
    }
    return sessionCache.get(cacheKey);
  }

  /**
   * Default token which will be used by the integration tests. If this property is set, then the
   * token from the request will be compared with this to perform authentication.
   */
  private String getFixedToken() {
    return operationsConfig.databaseConfig().fixedToken().orElse(null);
  }

  /**
   * Build key for CQLSession cache from tenant and token if the database type is AstraDB or from
   * tenant, username and password if the database type is OSS cassandra (also, if token is present
   * in the request, that will be given priority for the cache key).
   *
   * @return key for CQLSession cache
   */
  private SessionCacheKey getSessionCacheKey(String tenantId, String authToken) {
    var databaseConfig = operationsConfig.databaseConfig();

    // NOTE: this has changed, will create the UsernamePasswordCredentials from the token if that is
    // the token
    var credentials =
        CqlCredentials.create(
            getFixedToken(), authToken, databaseConfig.userName(), databaseConfig.password());

    // Only the OFFLINE_WRITER allows anonymous access, because it is not connecting to an actual
    // database
    if (credentials.isAnonymous()
        && !OFFLINE_WRITER.equals(operationsConfig.databaseConfig().type())) {
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          "Missing/Invalid authentication credentials provided for type: %s",
          operationsConfig.databaseConfig().type());
    }

    return switch (operationsConfig.databaseConfig().type()) {
      case CASSANDRA, OFFLINE_WRITER ->
          new SessionCacheKey(
              tenantId == null || tenantId.isBlank() ? DEFAULT_TENANT : tenantId, credentials);
      case ASTRA -> new SessionCacheKey(tenantId, credentials);
      default ->
          throw new IllegalStateException(
              "Unknown databaseConfig().type(): " + operationsConfig.databaseConfig().type());
    };
  }

  /**
   * Get cache size.
   *
   * @return cache size
   */
  public long cacheSize() {
    sessionCache.cleanUp();
    return sessionCache.estimatedSize();
  }

  /**
   * Remove CQLSession from cache.
   *
   * @param cacheKey key for CQLSession cache
   */
  public void removeSession(SessionCacheKey cacheKey) {
    sessionCache.invalidate(cacheKey);
    sessionCache.cleanUp();
    LOGGER.trace("Session removed for tenant : {}", cacheKey.tenantId());
  }

  /**
   * Put CQLSession in cache.
   *
   * @param sessionCacheKey key for CQLSession cache
   * @param cqlSession CQLSession instance
   */
  public void putSession(SessionCacheKey sessionCacheKey, CqlSession cqlSession) {
    sessionCache.put(sessionCacheKey, cqlSession);
  }

  /**
   * Key for CQLSession cache.
   *
   * <p>
   *
   * @param tenantId optional tenantId, if null converted to empty string
   * @param credentials Required, credentials for the session
   */
  public record SessionCacheKey(String tenantId, CqlCredentials credentials) {

    public SessionCacheKey {
      if (tenantId == null) {
        tenantId = "";
      }
      Objects.requireNonNull(credentials, "credentials must not be null");
    }
  }
}
