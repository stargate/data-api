package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.*;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.util.ClassUtils;
import io.stargate.sgv2.jsonapi.util.DynamicTTLCache;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

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
 * <p>The {@link DeactivatedTenantListener} interface will be called when a session is removed from
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
public class CQLSessionCache extends DynamicTTLCache<CQLSessionCache.SessionCacheKey, CqlSession> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CQLSessionCache.class);

  private final DynamicTTLSupplier ttlSupplier;

  private final CqlCredentialsFactory credentialsFactory;

  /**
   * Constructs a new instance of the {@link CQLSessionCache}.
   *
   * <p>Use this overload in production code, see other for detailed description of the parameters.
   */
  public CQLSessionCache(
      long cacheMaxSize,
      Duration cacheTTL,
      UserAgent slaUserAgent,
      Duration slaUserTTL,
      CqlCredentialsFactory credentialsFactory,
      SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      List<DeactivatedTenantListener> deactivatedTenantConsumer) {
    this(
        cacheMaxSize,
        cacheTTL,
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
      long cacheMaxSize,
      Duration cacheTTL,
      UserAgent slaUserAgent,
      Duration slaUserTTL,
      CqlCredentialsFactory credentialsFactory,
      SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      List<DeactivatedTenantListener> deactivatedTenantConsumer,
      boolean asyncTaskOnCaller,
      Ticker cacheTicker) {
    super(
        "cql_sessions_cache",
        cacheMaxSize,
        cacheKey -> sessionFactory.apply(cacheKey.tenant(), cacheKey.credentials()),
        buildCacheListeners(deactivatedTenantConsumer),
        meterRegistry,
        asyncTaskOnCaller,
        cacheTicker);

    Objects.requireNonNull(sessionFactory, "sessionFactory must not be null");
    this.credentialsFactory =
        Objects.requireNonNull(credentialsFactory, "credentialsFactory must not be null");
    this.ttlSupplier = new DynamicTTLSupplier(cacheTTL, slaUserAgent, slaUserTTL);

    LOGGER.info(
        "Initializing CQLSessionCache with cacheMaxSize={}, cacheTTL={}, slaUserAgent={}, slaUserTTL={}, deactivatedTenantConsumers.count={}",
        cacheTTL,
        cacheMaxSize,
        slaUserAgent,
        slaUserTTL,
        deactivatedTenantConsumer.size());
  }

  private static List<DynamicTTLCacheListener<SessionCacheKey, CqlSession>> buildCacheListeners(
      List<DeactivatedTenantListener> consumers) {

    List<DynamicTTLCacheListener<SessionCacheKey, CqlSession>> listeners = new ArrayList<>();
    listeners.add(new SessionCacheListener());

    if (consumers != null) {
      consumers.forEach(
          deactivedTenantListener -> {
            listeners.add(
                (key, value, cause) -> {
                  if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(
                        "Tenant deactivated, notifying consumer. tenantId={}, cause={}",
                        key.tenant(),
                        cause);
                  }

                  try {
                    deactivedTenantListener.accept(key.tenant());
                  } catch (Exception e) {
                    LOGGER.warn("Error calling deactivatedTenantListener for tenant={}, listener.class={}", key.tenant(), classSimpleName(deactivedTenantListener.getClass()), e);
                  }
                });
          });
    }
    return listeners;
  }

  /**
   * Gets or creates a {@link CqlSession} for the provided request context
   *
   * @param requestContext {@link RequestContext} to get the session for.
   * @return {@link CqlSession} for this tenant and credentials.
   */
  public Uni<CqlSession> getSession(RequestContext requestContext) {
    Objects.requireNonNull(requestContext, "requestContext must not be null");

    // Validation happens when creating the credentials and session key
    return getSession(
        requestContext.getTenant(),
        requestContext.getAuthToken(),
        requestContext.getUserAgent());
  }

  public Uni<CqlSession> getSession(CommandQueryExecutor.DBRequestContext requestContext) {
    Objects.requireNonNull(requestContext, "requestContext must not be null");

    // Validation happens when creating the credentials and session key
    return getSession(
        requestContext.tenant(),
        requestContext.authToken(),
        requestContext.userAgent());
  }

  /**
   * Retrieves or creates a {@link CqlSession} for the specified tenant and authentication token.
   *
   * <p>If the database type is OFFLINE_WRITER, this method will attempt to retrieve the session
   * from the cache without creating a new session if it is not present. For other database types, a
   * new session will be created if it is not already cached.
   *
   * @param tenant the identifier for the tenant
   * @param authToken the authentication token for accessing the session
   * @param userAgent Nullable user agent, if matching the configured SLA checker user agent then
   *     the session will use the TTL for the SLA user.
   * @return a {@link CqlSession} associated with the given tenant and authToken
   */
  public Uni<CqlSession> getSession(Tenant tenant, String authToken, UserAgent userAgent) {
    return get(createCacheKey(tenant, authToken, userAgent));
  }

  /**
   * For testing, peek into the cache to see if a session is present for the given tenant,
   * authToken, and userAgent.
   */
  @VisibleForTesting
  protected Optional<CqlSession> peekSession(Tenant tenant, String authToken, UserAgent userAgent) {
    return getIfPresent(createCacheKey(tenant, authToken, userAgent));
  }

  /** Builds the cache key to use for the supplied tenant and authentication token. */
  private SessionCacheKey createCacheKey(Tenant tenant, String authToken, UserAgent userAgent) {

    Objects.requireNonNull(tenant, "tenant must not be null");
    Objects.requireNonNull(authToken, "authToken must not be null");
    Objects.requireNonNull(userAgent, "userAgent must not be null");

    var credentials = credentialsFactory.apply(authToken);
    if (credentials == null) {
      // sanity check
      throw new IllegalStateException("credentialsFactory returned null");
    }

    return new SessionCacheKey(tenant, credentials, ttlSupplier.ttlForUsageAgent(userAgent), userAgent);
  }

  /** Key for CQLSession cache. */
  static class SessionCacheKey implements DynamicTTLCache.CacheKey {

    private final Tenant tenant;
    private final CqlCredentials credentials;
    private final Duration ttl;
    // user agent only added for logging and debugging
    private final UserAgent userAgent;

    /**
     * Creates a new instance of {@link SessionCacheKey}.
     *
     * @param tenant The identifier for the tenant.
     * @param credentials The credentials used for authentication.
     * @param ttl The time-to-live (TTL) duration for the cache entry. Note: This is NOT used in the
     *     value quality of the cache key, it is set so dynamic TTL can be used per key.
     * @param userAgent Optional user agent for the request, not used in the equality of the key
     *     just for logging.
     */
    SessionCacheKey(
        Tenant tenant,
        CqlCredentials credentials,
        Duration ttl,
        UserAgent userAgent) {

      // tenant is only used to identify the session, not passed to backend db
      // normalising the unset value to null
      this.tenant = Objects.requireNonNull(tenant, "tenant must not be null");
      this.credentials = Objects.requireNonNull(credentials, "credentials must not be null");
      this.ttl = Objects.requireNonNull(ttl, "ttl must not be null");
      this.userAgent = Objects.requireNonNull(userAgent, "userAgent must not be null");
    }

    Tenant tenant() {
      return tenant;
    }

    CqlCredentials credentials() {
      return credentials;
    }

    @Override
    public Duration ttl() {
      return ttl;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SessionCacheKey that)) {
        return false;
      }
      return tenant.equals(that.tenant) && credentials.equals(that.credentials);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tenant, credentials);
    }

    @Override
    public String toString() {
      return new StringBuilder("SessionCacheKey{")
          .append("tenant='")
          .append(tenant)
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
   * Listens for changes from the underlying dynamic TTL cache, and does things we need as a CQL
   * Session Cache
   */
  static class SessionCacheListener
      implements DynamicTTLCacheListener<SessionCacheKey, CqlSession> {

    @Override
    public void onRemoved(SessionCacheKey key, CqlSession value, RemovalCause cause) {
      // we need to manually close the session, the cache will not close it for us.

      if (value == null) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("CQL Session was null when removing from cache, key={}", key);
        }
        return;
      }

      // This will be running on a cache tread, any error will not make it to the user
      // So we log errors and swallow
      // getting name from the session now so we don't touch it after closing it, may not be reliable
      var sessionName = value.getName();
      try {
        value.close();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Closed CQL Session session.name={}", sessionName);
        }
      } catch (Exception e) {
        LOGGER.error("Error closing CQLSession session.name={}", sessionName, e);
      }
    }
  }

  /** Callback when a tenant is deactivated. */
  @FunctionalInterface
  public interface DeactivatedTenantListener extends Consumer<Tenant> {
    void accept(Tenant tenant);
  }

  /** Called to create credentials used with the session and session cache key. */
  @FunctionalInterface
  public interface CredentialsFactory extends Function<String, CqlCredentials> {
    CqlCredentials apply(String authToken);
  }

  /** Called to create a new session when one is needed. */
  @FunctionalInterface
  public interface SessionFactory
      extends BiFunction<Tenant, CqlCredentials, CompletionStage<CqlSession>> {
    CompletionStage<CqlSession> apply(Tenant tenant, CqlCredentials credentials);
  }
}
