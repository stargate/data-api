package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache for managing and reusing {@link CqlSession} instances based on tenant and authentication
 * credentials.
 *
 * <p>Sessions are cached based on the tenantId and authentication token. So that a single tenant
 * may have multiple sessions, but a single session is used for the same tenant and auth token.
 *
 * <p>Create instances using the {@link CqlSessionCacheSupplier} class.
 *
 * <p>Call {@link #getSession(RequestContext)} and overloads to get a session for the current
 * request context.
 *
 * <p>The {@link DeactivatedTenantConsumer} interface will be called when a session is removed from
 * the cache, so that schema cache and metrics can be updated to remove the tenant. NOTE: this is
 * called when the session expires, but a single tenant may have multiple sessions (based on key
 * above), so it is not a guarantee that the tenant is not active with another set of credentials.
 * If you take action to remove a deactivated tenant, there should be a path for the tenant to be
 * reactivated.
 */
public class CQLSessionCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(CQLSessionCache.class);

  /**
   * Default tenant to be used when the backend is OSS cassandra and when no tenant is passed in the
   * request
   */
  public static final String DEFAULT_TENANT = "default_tenant";

  private final DatabaseType databaseType;

  private final LoadingCache<SessionCacheKey, CqlSession> sessionCache;
  private final CqlCredentialsFactory credentialsFactory;
  private final SessionFactory sessionFactory;

  private List<DeactivatedTenantConsumer> deactivatedTenantConsumers;

  public CQLSessionCache(
      DatabaseType databaseType,
      Duration cacheTTL,
      long cacheMaxSize,
      CqlCredentialsFactory credentialsFactory,
      SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      List<DeactivatedTenantConsumer> deactivatedTenantConsumer) {
    this(
        databaseType,
        cacheTTL,
        cacheMaxSize,
        credentialsFactory,
        sessionFactory,
        meterRegistry,
        deactivatedTenantConsumer,
        false);
  }

  /**
   * Constructs a new instance of the {@link CQLSessionCache}.
   *
   * @param databaseType The type of database being used,
   * @param cacheTTL The time-to-live (TTL) duration for cache entries.
   * @param cacheMaxSize The maximum size of the cache.
   * @param credentialsFactory A factory for creating {@link CqlCredentials} based on authentication
   *     tokens.
   * @param sessionFactory A factory for creating new {@link CqlSession} instances when needed.
   * @param meterRegistry The {@link MeterRegistry} for monitoring cache metrics.
   * @param deactivatedTenantConsumer A list of consumers to handle tenant deactivation events.
   * @param asyncTaskOnCaller If true, asynchronous tasks (e.g., callbacks) will run on the caller
   *     thread. This is intended for testing purposes only. DO NOT USE in production.
   */
  CQLSessionCache(
      DatabaseType databaseType,
      Duration cacheTTL,
      long cacheMaxSize,
      CqlCredentialsFactory credentialsFactory,
      SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      List<DeactivatedTenantConsumer> deactivatedTenantConsumer,
      boolean asyncTaskOnCaller) {

    this.databaseType = Objects.requireNonNull(databaseType, "databaseType must not be null");
    this.credentialsFactory =
        Objects.requireNonNull(credentialsFactory, "credentialsFactory must not be null");
    this.sessionFactory = Objects.requireNonNull(sessionFactory, "sessionFactory must not be null");

    this.deactivatedTenantConsumers =
        deactivatedTenantConsumer == null ? List.of() : List.copyOf(deactivatedTenantConsumer);

    LOGGER.info(
        "Initializing CQLSessionCache with cacheTTL={}, cacheMaxSize={}, databaseType={}, deactivatedTenantConsumers.count={}",
        cacheTTL,
        cacheMaxSize,
        databaseType,
        deactivatedTenantConsumers.size());

    // setting both expireAfterAccess and expireAfterWrite so that if a session is added and never
    // accessed, it will still be removed after the same TTL. expireAfterAccess only works
    // if the session is accessed.
    var builder =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheTTL)
            .expireAfterWrite(cacheTTL)
            .maximumSize(cacheMaxSize)
            .removalListener(this::onKeyRemoved)
            .recordStats();
    if (asyncTaskOnCaller) {
      LOGGER.warn(
          "CQLSessionCache CONFIGURED TO RUN ASYNC TASKS SUCH AS CALLBACKS ON THE CALLER THREAD, DO NOT USE IN PRODUCTION.");
      builder = builder.executor(Runnable::run);
    }
    LoadingCache<SessionCacheKey, CqlSession> loadingCache = builder.build(this::onLoadSession);
    this.sessionCache =
        CaffeineCacheMetrics.monitor(meterRegistry, loadingCache, "cql_sessions_cache");
  }

  /**
   * Gets or creates a {@link CqlSession} for the provided request context
   *
   * @param requestContext {@link RequestContext} to get the session for.
   * @return {@link CqlSession} for this tenant and credentials.
   */
  public CqlSession getSession(RequestContext requestContext) {
    Objects.requireNonNull(requestContext, "requestContext must not be null");

    // Validation happens when creating the credentials and session key
    return getSession(
        requestContext.getTenantId().orElse(""), requestContext.getCassandraToken().orElse(""));
  }

  /**
   * Retrieves or creates a {@link CqlSession} for the specified tenant and authentication token.
   *
   * <p>If the database type is OFFLINE_WRITER, this method will attempt to retrieve the session
   * from the cache without creating a new session if it is not present. For other database types, a
   * new session will be created if it is not already cached.
   *
   * @param tenantId the identifier for the tenant
   * @param authToken the authentication token for accessing the session
   * @return a {@link CqlSession} associated with the given tenantId and authToken
   */
  public CqlSession getSession(String tenantId, String authToken) {

    var cacheKey = createCacheKey(tenantId, authToken);
    // TODO: why is this different for OFFLINE ?
    return switch (databaseType) {
      case OFFLINE_WRITER -> sessionCache.getIfPresent(cacheKey);
      default -> sessionCache.get(cacheKey);
    };
  }

  /** Process a key being removed from the cache for any reason. */
  private void onKeyRemoved(SessionCacheKey cacheKey, CqlSession session, RemovalCause cause) {

    deactivatedTenantConsumers.forEach(
        consumer -> {
          try {
            consumer.accept(cacheKey.tenantId(), cause);
          } catch (Exception e) {
            LOGGER.warn(
                "Error calling deactivated tenant consumer: tenantId={}, cause={}, consumer.class={}",
                cacheKey.tenantId(),
                cause,
                classSimpleName(consumer.getClass()),
                e);
          }
        });

    // we need to manually close the session, the cache will not close it for us.
    if (session != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Closing CQL Session tenantId={}, credentials.class={}, credentials.isAnonymous={}",
            cacheKey.tenantId(),
            classSimpleName(cacheKey.credentials().getClass()),
            cacheKey.credentials().isAnonymous());
      }

      // This will be running on a cache tread, any error will not make it to the user
      // So we log it and swallow
      try {
        session.close();
      } catch (Exception e) {
        LOGGER.error(
            "Error closing CQLSession tenantId={}, credentials.class={}, credentials.isAnonymous={}",
            cacheKey.tenantId(),
            classSimpleName(cacheKey.credentials().getClass()),
            cacheKey.credentials().isAnonymous(),
            e);
      }

    } else if (LOGGER.isWarnEnabled()) {
      LOGGER.warn(
          "CQL Session was null when removing from cache, tenantId={}, credentials.class={}, credentials.isAnonymous={}",
          cacheKey.tenantId(),
          classSimpleName(cacheKey.credentials().getClass()),
          cacheKey.credentials().isAnonymous());
    }
  }

  /** Callback to create a new session when one is needed for the cache */
  private CqlSession onLoadSession(SessionCacheKey cacheKey) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Opening CQL Session tenantId={}, credentials.class={}, credentials.isAnonymous={}",
          cacheKey.tenantId(),
          classSimpleName(cacheKey.credentials().getClass()),
          cacheKey.credentials().isAnonymous());
    }

    return sessionFactory.apply(cacheKey.tenantId(), cacheKey.credentials());
  }

  /** Builds the cache key to use for the supplied tenant and authentication token. */
  private SessionCacheKey createCacheKey(String tenantId, String authToken) {

    var credentials = credentialsFactory.apply(authToken);
    if (credentials == null) {
      // sanity check
      throw new IllegalStateException("credentialsFactory returned null");
    }

    return switch (databaseType) {
      case CASSANDRA, OFFLINE_WRITER ->
          new SessionCacheKey(
              tenantId == null || tenantId.isBlank() ? DEFAULT_TENANT : tenantId, credentials);
      case ASTRA -> new SessionCacheKey(tenantId, credentials);
    };
  }

  /**
   * Gets the current size of the cache, note this causes bookkeeping to happen so best not done in
   * production code
   */
  @VisibleForTesting
  long cacheSize() {
    sessionCache.cleanUp();
    return sessionCache.estimatedSize();
  }

  /**
   * Invalidate all entries and cleanup, for testing when items are invalidated.
   *
   * <p>Note: Removal cause will be {@link RemovalCause#EXPLICIT} for items.
   */
  @VisibleForTesting
  void clearCache() {
    LOGGER.info("Manually clearing CQLSession cache");
    sessionCache.invalidateAll();
    sessionCache.cleanUp();
  }

  /**
   * Clean up the cache, for testing when items are invalidated. The cache will try to be lazy, so
   * things like evictions may not happen exactly at the TTL, this is a way to force it.
   */
  @VisibleForTesting
  void cleanUp() {
    sessionCache.cleanUp();
  }

  /**
   * Key for CQLSession cache.
   *
   * <p>As the cache key, we rely on the record implementation of hash and equals.
   *
   * @param tenantId optional tenantId, if null or blank converted to empty string
   * @param credentials Required, credentials for the session
   */
  public record SessionCacheKey(String tenantId, CqlCredentials credentials) {

    public SessionCacheKey {
      if (tenantId == null || tenantId.isBlank()) {
        tenantId = "";
      }
      Objects.requireNonNull(credentials, "credentials must not be null");
    }
  }

  /** Callback when a tenant is deactivated. */
  @FunctionalInterface
  public interface DeactivatedTenantConsumer extends BiConsumer<String, RemovalCause> {
    void accept(String tenantId, RemovalCause cause);
  }

  /** Called to create credentials used with the session and session cache key. */
  @FunctionalInterface
  public interface CredentialsFactory extends Function<String, CqlCredentials> {
    CqlCredentials apply(String authToken);
  }

  /** Called to create a new session when one is needed. */
  @FunctionalInterface
  public interface SessionFactory extends BiFunction<String, CqlCredentials, CqlSession> {
    CqlSession apply(String tenantId, CqlCredentials credentials);
  }
}
