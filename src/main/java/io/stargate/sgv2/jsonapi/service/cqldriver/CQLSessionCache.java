package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricNames.CQL_SESSION_CACHE_EXPLICIT_EVICTION_METRICS;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.CQL_SESSION_CACHE_NAME_TAG;
import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.*;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
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
 *
 * <p><b>NOTE:</b> There is no method to get the size of the cache because it is not a reliable
 * measure, it's only an estimate. We can assume the size feature works. For testing use {@link
 * #peekSession(String, String, String)}
 */
public class CQLSessionCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(CQLSessionCache.class);

  /**
   * Default tenant to be used when the backend is OSS cassandra and when no tenant is passed in the
   * request
   */
  public static final String DEFAULT_TENANT = "default_tenant";

  private final DatabaseType databaseType;
  private final Duration cacheTTL;
  private final String slaUserAgent;
  private final Duration slaUserTTL;

  private final LoadingCache<SessionCacheKey, SessionValueHolder> sessionCache;
  private final CqlCredentialsFactory credentialsFactory;
  private final SessionFactory sessionFactory;

  private List<DeactivatedTenantConsumer> deactivatedTenantConsumers;

  private MeterRegistry meterRegistry;

  /**
   * Constructs a new instance of the {@link CQLSessionCache}.
   *
   * <p>Use this overload in production code, see other for detailed description of the parameters.
   */
  public CQLSessionCache(
      DatabaseType databaseType,
      Duration cacheTTL,
      long cacheMaxSize,
      String slaUserAgent,
      Duration slaUserTTL,
      CqlCredentialsFactory credentialsFactory,
      SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      List<DeactivatedTenantConsumer> deactivatedTenantConsumer) {
    this(
        databaseType,
        cacheTTL,
        cacheMaxSize,
        slaUserAgent,
        slaUserTTL,
        credentialsFactory,
        sessionFactory,
        meterRegistry,
        deactivatedTenantConsumer,
        false,
        null);
  }

  /**
   * Constructs a new instance of the {@link CQLSessionCache}.
   *
   * <p>Use this ctor for testing only.
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
   * @param cacheTicker If non-null, this is the ticker used by the cache to decide when to expire
   *     entries. If null, the default ticker is used. DO NOT USE in production.
   */
  CQLSessionCache(
      DatabaseType databaseType,
      Duration cacheTTL,
      long cacheMaxSize,
      String slaUserAgent,
      Duration slaUserTTL,
      CqlCredentialsFactory credentialsFactory,
      SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      List<DeactivatedTenantConsumer> deactivatedTenantConsumer,
      boolean asyncTaskOnCaller,
      Ticker cacheTicker) {

    this.databaseType = Objects.requireNonNull(databaseType, "databaseType must not be null");
    this.cacheTTL = Objects.requireNonNull(cacheTTL, "cacheTTL must not be null");
    // we use case-insensitive compare
    this.slaUserAgent = slaUserAgent == null || slaUserAgent.isBlank() ? null : slaUserAgent;
    if (slaUserAgent != null) {
      this.slaUserTTL =
          Objects.requireNonNull(slaUserTTL, "slaUserTTL must not be null is slaUserAgent is set");
    } else {
      this.slaUserTTL = null;
    }

    this.credentialsFactory =
        Objects.requireNonNull(credentialsFactory, "credentialsFactory must not be null");
    this.sessionFactory = Objects.requireNonNull(sessionFactory, "sessionFactory must not be null");
    this.deactivatedTenantConsumers =
        deactivatedTenantConsumer == null ? List.of() : List.copyOf(deactivatedTenantConsumer);
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");

    LOGGER.info(
        "Initializing CQLSessionCache with cacheTTL={}, cacheMaxSize={}, databaseType={}, slaUserAgent={}, slaUserTTL={}, deactivatedTenantConsumers.count={}",
        cacheTTL,
        cacheMaxSize,
        databaseType,
        slaUserAgent,
        slaUserTTL,
        deactivatedTenantConsumers.size());

    CaffeineStatsCounter caffeineStatsCounter =
        new CaffeineStatsCounter(meterRegistry, CQL_SESSION_CACHE_NAME_TAG);

    var builder =
        Caffeine.newBuilder()
            .expireAfter(new SessionExpiry())
            .maximumSize(cacheMaxSize)
            .removalListener(this::onKeyRemoved)
            .recordStats(() -> caffeineStatsCounter);

    if (asyncTaskOnCaller) {
      LOGGER.warn(
          "CQLSessionCache CONFIGURED TO RUN ASYNC TASKS SUCH AS CALLBACKS ON THE CALLER THREAD, DO NOT USE IN PRODUCTION.");
      builder = builder.executor(Runnable::run);
    }
    if (cacheTicker != null) {
      LOGGER.warn("CQLSessionCache CONFIGURED TO USE A CUSTOM TICKER, DO NOT USE IN PRODUCTION.");
      builder = builder.ticker(cacheTicker);
    }

    this.sessionCache = builder.build(this::onLoadSession);

    caffeineStatsCounter.registerSizeMetric(sessionCache);
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
        requestContext.getTenantId().orElse(""),
        requestContext.getCassandraToken().orElse(""),
        requestContext.getUserAgent().orElse(null));
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
   * @param userAgent Nullable user agent, if matching the configured SLA checker user agent then
   *     the session will use the TTL for the SLA user.
   * @return a {@link CqlSession} associated with the given tenantId and authToken
   */
  public CqlSession getSession(String tenantId, String authToken, String userAgent) {

    var cacheKey = createCacheKey(tenantId, authToken, userAgent);
    // TODO: why is this different for OFFLINE ?
    var holder =
        switch (databaseType) {
          case OFFLINE_WRITER -> sessionCache.getIfPresent(cacheKey);
          default -> sessionCache.get(cacheKey);
        };
    return holder == null ? null : holder.session();
  }

  /**
   * Evicts a session from the cache based on the provided {@link RequestContext}.
   *
   * @see #evictSession(String, String, String, EvictSessionCause) for details on eviction.
   * @param requestContext The request context containing tenant, auth, and user agent info.
   * @return {@code true} if a session was evicted, {@code false} otherwise.
   */
  public boolean evictSession(RequestContext requestContext, EvictSessionCause cause) {
    Objects.requireNonNull(requestContext, "requestContext must not be null for eviction");

    // Validation happens when creating the credentials and session key
    return evictSession(
        requestContext.getTenantId().orElse(""),
        requestContext.getCassandraToken().orElse(""),
        requestContext.getUserAgent().orElse(null),
        cause);
  }

  /**
   * Evicts a session from the cache programmatically. This is intended for use in scenarios where a
   * session is known to be in an unrecoverable state (e.g., after all cluster nodes restart) and
   * needs to be forcibly removed to allow for a fresh connection on the next request.
   *
   * @param tenantId the identifier for the tenant
   * @param authToken the authentication token for accessing the session
   * @param userAgent Nullable user agent
   * @return {@code true} if a session was evicted, {@code false} otherwise.
   */
  public boolean evictSession(
      String tenantId, String authToken, String userAgent, EvictSessionCause cause) {

    meterRegistry
        .summary(
            CQL_SESSION_CACHE_EXPLICIT_EVICTION_METRICS,
            "CQL_SESSION_CACHE_EXPLICIT_EVICTION_CAUSE_TAG",
            cause.name())
        .record(1);

    var cacheKey = createCacheKey(tenantId, authToken, userAgent);

    // Invalidate the key. This will trigger the onKeyRemoved listener,
    // which will close the CqlSession and run other cleanup.
    // Use .asMap().remove() as the author suggested:
    // https://stackoverflow.com/questions/67994799/how-do-i-make-invalidate-an-entry-and-return-its-value-from-a-caffeine-cache
    boolean entryFound = sessionCache.asMap().remove(cacheKey) != null;
    LOGGER.warn(
        "Explicitly evicted session from cache. Cache Key: {} (entry found: {})",
        cacheKey,
        entryFound);
    return entryFound;
  }

  public enum EvictSessionCause {
    /**
     * It indicates that the DB session used is no longer reliable and should be terminated (e.g.
     * after all cluster nodes restart), so that a fresh connection is created on the next request.
     */
    UNRELIABLE_DB_SESSION,

    TEST
  }

  /**
   * For testing, peek into the cache to see if a session is present for the given tenantId,
   * authToken, and userAgent.
   */
  @VisibleForTesting
  protected Optional<CqlSession> peekSession(String tenantId, String authToken, String userAgent) {
    var cacheKey = createCacheKey(tenantId, authToken, userAgent);
    return Optional.ofNullable(sessionCache.getIfPresent(cacheKey))
        .map(SessionValueHolder::session);
  }

  /** Process a key being removed from the cache for any reason. */
  private void onKeyRemoved(
      SessionCacheKey cacheKey, SessionValueHolder sessionHolder, RemovalCause cause) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("onKeyRemoved for sessionHolder={}, cause={}", sessionHolder, cause);
    }

    deactivatedTenantConsumers.forEach(
        consumer -> {
          try {
            consumer.accept(cacheKey.tenantId(), cause);
          } catch (Exception e) {
            LOGGER.warn(
                "Error calling deactivated tenant consumer: sessionHolder={}, cause={}, consumer.class={}",
                sessionHolder,
                cause,
                classSimpleName(consumer.getClass()),
                e);
          }
        });

    // we need to manually close the session, the cache will not close it for us.
    if (sessionHolder != null) {
      // This will be running on a cache tread, any error will not make it to the user
      // So we log it and swallow
      try {
        sessionHolder.session.close();
      } catch (Exception e) {
        LOGGER.error("Error closing CQLSession sessionHolder={}", sessionHolder, e);
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Closed CQL Session sessionHolder={}", sessionHolder);
      }

    } else if (LOGGER.isWarnEnabled()) {
      LOGGER.warn("CQL Session was null when removing from cache, cacheKey={}", cacheKey);
    }
  }

  /** Callback to create a new session when one is needed for the cache */
  private SessionValueHolder onLoadSession(SessionCacheKey cacheKey) {

    // factory will do some logging
    var holder =
        new SessionValueHolder(
            sessionFactory.apply(cacheKey.tenantId(), cacheKey.credentials()), cacheKey);

    if (LOGGER.isDebugEnabled()) {
      // so we get the identity hash code of the session holder
      LOGGER.debug("Loaded CQLSession into cache, holder={}", holder);
    }
    return holder;
  }

  /** Builds the cache key to use for the supplied tenant and authentication token. */
  private SessionCacheKey createCacheKey(String tenantId, String authToken, String userAgent) {

    var credentials = credentialsFactory.apply(authToken);
    if (credentials == null) {
      // sanity check
      throw new IllegalStateException("credentialsFactory returned null");
    }

    // userAgent arg can be null
    // slaUserAgent forced to lower case in the ctor
    var keyTTL =
        slaUserAgent == null || !slaUserAgent.equalsIgnoreCase(userAgent) ? cacheTTL : slaUserTTL;

    return switch (databaseType) {
      case CASSANDRA, OFFLINE_WRITER ->
          new SessionCacheKey(
              tenantId == null || tenantId.isBlank() ? DEFAULT_TENANT : tenantId,
              credentials,
              keyTTL,
              userAgent);
      case ASTRA -> new SessionCacheKey(tenantId, credentials, keyTTL, userAgent);
    };
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

  /** Key for CQLSession cache. */
  static class SessionCacheKey {

    private final String tenantId;
    private final CqlCredentials credentials;
    private final Duration ttl;
    // user agent only added for logging and debugging
    @Nullable private final String userAgent;

    /**
     * Creates a new instance of {@link SessionCacheKey}.
     *
     * @param tenantId The identifier for the tenant.
     * @param credentials The credentials used for authentication.
     * @param ttl The time-to-live (TTL) duration for the cache entry. Note: This is NOT used in the
     *     value quality of the cache key, it is set so dynamic TTL can be used per key.
     * @param userAgent Optional user agent for the request, not used in the equality of the key
     *     just for logging.
     */
    SessionCacheKey(
        String tenantId, CqlCredentials credentials, Duration ttl, @Nullable String userAgent) {
      if (tenantId == null || tenantId.isBlank()) {
        tenantId = "";
      }
      this.tenantId = tenantId;
      this.credentials = Objects.requireNonNull(credentials, "credentials must not be null");
      this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
      this.userAgent = userAgent;
    }

    public String tenantId() {
      return tenantId;
    }

    public CqlCredentials credentials() {
      return credentials;
    }

    public Duration ttl() {
      return ttl;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SessionCacheKey)) {
        return false;
      }
      SessionCacheKey that = (SessionCacheKey) o;
      return tenantId.equals(that.tenantId) && credentials.equals(that.credentials);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tenantId, credentials);
    }

    @Override
    public String toString() {
      return new StringBuilder("SessionCacheKey{")
          .append("tenantId='")
          .append(tenantId)
          .append('\'')
          .append(", credentials=")
          .append(credentials) // creds should make sure they dont include sensitive info
          .append(", ttl=")
          .append(ttl)
          .append(", userAgent='")
          .append(userAgent)
          .append('}')
          .toString();
    }
  }

  /**
   * Holder for the value added to the cache to make it very clear what key was used when it was
   * added so we can get the TTL used when it was loaded. Used for dynamic TTL in the Expiry class
   */
  record SessionValueHolder(CqlSession session, SessionCacheKey loadingKey) {

    SessionValueHolder {
      Objects.requireNonNull(session, "session must not be null");
      Objects.requireNonNull(loadingKey, "loadingKey must not be null");
    }

    /**
     * Note that the cache can decide when it wants to actually remove an expired key, so we may be
     * closing a session for a tenant at the same time we are opening one. The {@link #toString()}
     * includes the identity hash code to help with debugging.
     */
    @Override
    public String toString() {
      return new StringBuilder("SessionValueHolder{")
          .append("identityHashCode=")
          .append(System.identityHashCode(this))
          .append(", loadingKey=")
          .append(loadingKey)
          .append('}')
          .toString();
    }
  }

  /**
   * Dynamic cache TTL for the session cache.
   *
   * <p>We use the maximum TTL between either the key that was used the load the session, or the
   * current key being used to access it. The TTL is set when the key is created based on the user
   * agent coming in. So if a SLA user agent adds it, then a non SLA uses it the non SLA user agent
   * TTL will be used.
   *
   * <p>The laster user who access the session will set the TTL for the session if their TTL is
   * higher.
   */
  static class SessionExpiry implements Expiry<SessionCacheKey, SessionValueHolder> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionExpiry.class);

    @Override
    public long expireAfterCreate(SessionCacheKey key, SessionValueHolder value, long currentTime) {
      return value.loadingKey().ttl().toNanos();
    }

    @Override
    public long expireAfterUpdate(
        SessionCacheKey key, SessionValueHolder value, long currentTime, long currentDuration) {
      return currentDuration;
    }

    @Override
    public long expireAfterRead(
        SessionCacheKey key, SessionValueHolder value, long currentTime, long currentDuration) {
      long accessTTL = key.ttl().toNanos();
      long loadTTL = value.loadingKey().ttl().toNanos();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "expireAfterRead() - key.tenant={}, key.ttl={}, key.identityHashCode={}, value.loadingKey.ttl={}, value.loadingKey.identityHashCode={}",
            key.tenantId(),
            key.ttl(),
            System.identityHashCode(key),
            value.loadingKey.ttl(),
            System.identityHashCode(value.loadingKey));
      }
      return Math.max(accessTTL, loadTTL);
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
