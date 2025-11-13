package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.exception.CassandraAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

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

    /**
     * Private Static logger for the class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLSessionCache.class);

    public static final String DEFAULT_TENANT = "default_tenant";

    /**
     * Circuit breaker configuration (return error for problematic keys)
     */
    private static final int MAX_CONSECUTIVE_FAILURES = 3;
    private static final Duration CIRCUIT_BREAKER_RESET_TIMEOUT = Duration.ofMinutes(5);
    private final ConcurrentHashMap<CQLSessionCacheKey, CircuitBreakerState> circuitBreakers =
            new ConcurrentHashMap<>();

    /**
     * Factories
     */
    private final CqlCredentialsFactory credentialsFactory;
    private final SessionFactory sessionFactory;

    /**
     * Our bad Guys
     */
    private final List<DeactivatedTenantConsumer> deactivatedTenantConsumers;

    private final DatabaseType databaseType;
    private final Duration cacheTTL;
    private final String slaUserAgent;
    private final Duration slaUserTTL;

    /**
     * Our internal beloved cache.
     */
    private final LoadingCache<CQLSessionCacheKey, SessionValueHolder> sessionCache;

    /**
     * Important !
     * Striped locks to prevent concurrent loads for the same key
     * This is to prevent for denial of services where all threads are locked
     */
    private final Striped<Lock> sessionLoadLocks = Striped.lock(128);

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
        this.slaUserAgent = slaUserAgent == null || slaUserAgent.isBlank() ? null : slaUserAgent;
        if (slaUserAgent != null) {
            this.slaUserTTL = Objects
              .requireNonNull(slaUserTTL, "slaUserTTL must not be null is slaUserAgent is set");
        } else {
            this.slaUserTTL = null;
        }

        this.credentialsFactory =
                Objects.requireNonNull(credentialsFactory, "credentialsFactory must not be null");
        this.sessionFactory = Objects.requireNonNull(sessionFactory, "sessionFactory must not be null");
        this.deactivatedTenantConsumers =
                deactivatedTenantConsumer == null ? List.of() : List.copyOf(deactivatedTenantConsumer);

        LOGGER.info(
                "Initializing CQLSessionCache with cacheTTL={}, cacheMaxSize={}, databaseType={}, slaUserAgent={}, slaUserTTL={}, deactivatedTenantConsumers.count={}, maxFailures={}, circuitBreakerResetTimeout={}",
                cacheTTL,
                cacheMaxSize,
                databaseType,
                slaUserAgent,
                slaUserTTL,
                deactivatedTenantConsumers.size(),
                MAX_CONSECUTIVE_FAILURES,
                CIRCUIT_BREAKER_RESET_TIMEOUT);

        // Builder for internal cache
        Caffeine<CQLSessionCacheKey, SessionValueHolder> builder =
                Caffeine.newBuilder()
                        .expireAfter(new SessionExpiry())
                        .maximumSize(cacheMaxSize)
                        .removalListener(this::onKeyRemoved)
                        .recordStats();

        // ==============================================
        // Keep it ? Aaron why this ?
        // ==============================================

        if (asyncTaskOnCaller) {
            LOGGER.warn(
                    "CQLSessionCache CONFIGURED TO RUN ASYNC TASKS SUCH AS CALLBACKS ON THE CALLER THREAD, DO NOT USE IN PRODUCTION.");
            builder = builder.executor(Runnable::run);
        }
        if (cacheTicker != null) {
            LOGGER.warn("CQLSessionCache CONFIGURED TO USE A CUSTOM TICKER, DO NOT USE IN PRODUCTION.");
            builder = builder.ticker(cacheTicker);
        }

        // ==============================================

        LoadingCache<CQLSessionCacheKey, SessionValueHolder> loadingCache = builder
          .build(this::onLoadSession);

        this.sessionCache = CaffeineCacheMetrics
          .monitor(meterRegistry, loadingCache, "cql_sessions_cache");
    }

    public CqlSession getSession(RequestContext requestContext) {
        Objects.requireNonNull(requestContext, "requestContext must not be null");
        return getSession(
                requestContext.getTenantId().orElse(""),
                requestContext.getCassandraToken().orElse(""),
                requestContext.getUserAgent().orElse(null));
    }

    /**
     * Retrieves or creates a {@link CqlSession} with protection against:
     * 1. Concurrent loads for the same key (request coalescing)
     * 2. Repeated failures (circuit breaker)
     */
    public CqlSession getSession(String tenantId, String authToken, String userAgent) {

        CQLSessionCacheKey cacheKey =
                createCacheKey(tenantId, authToken, userAgent);

        // Fast path: check if already in cache (no lock needed)
        var holder = sessionCache.getIfPresent(cacheKey);

        if (holder != null) {
            // Reset circuit breaker on successful cache hit
            resetCircuitBreakerIfNeeded(cacheKey);
            return holder.session();
        }

        // For OFFLINE_WRITER, don't create new sessions
        if (databaseType == DatabaseType.OFFLINE_WRITER) {
            return null;
        }

        // Check circuit breaker BEFORE attempting to load
        checkCircuitBreaker(cacheKey);

        // Acquire lock for this specific cache key to prevent concurrent loads
        Lock lock = sessionLoadLocks.get(cacheKey);
        lock.lock();
        try {

            // Double-check cache after acquiring lock (another thread may have loaded it)
            holder = sessionCache.getIfPresent(cacheKey);
            if (holder != null) {
                LOGGER.debug(
                        "Session loaded by another thread while waiting for lock. key={}", cacheKey);
                resetCircuitBreakerIfNeeded(cacheKey);
                return holder.session();
            }

            // We're the first thread to load this key
            LOGGER.info("Loading new session under lock for key={}", cacheKey);

            long startTime = System.currentTimeMillis();
            try {
                holder = sessionCache.get(cacheKey); // Calls onLoadSession
                long loadTime = System.currentTimeMillis() - startTime;

                LOGGER.info("Successfully loaded session for key={}, loadTime={}ms", cacheKey, loadTime);

                // Success - reset circuit breaker
                recordSuccess(cacheKey);

                return holder == null ? null : holder.session();

            } catch (Exception e) {
                long loadTime = System.currentTimeMillis() - startTime;
                LOGGER.error(
                        "Failed to load session for key={}, loadTime={}ms", cacheKey, loadTime, e);

                // Record failure in circuit breaker
                recordFailure(cacheKey, e);
                throw CassandraAuthenticationException.Code.AUTHENTICATION_FAILED.get(
                        Map.of("tenantId", tenantId)
                );

            }

        } finally {
            lock.unlock();
        }
    }

    /**
     * Check if circuit breaker is open for this key and throw exception if so.
     *
     * @param cacheKey
     *      cacheKey representing a Session
     */
    private void checkCircuitBreaker(CQLSessionCacheKey cacheKey) {
        CircuitBreakerState state = circuitBreakers.get(cacheKey);
        // As state is null, no failures yet
        if (state == null) {
            return;
        }

        // Check if circuit breaker should be reset
        if (state.shouldReset()) {
            LOGGER.info("Circuit breaker reset timeout expired for key={}", cacheKey);
            circuitBreakers.remove(cacheKey);
            return;
        }

        // Check if circuit is open
        if (state.isOpen()) {
            String message = String.format(
                    "Circuit breaker OPEN for tenant=%s, consecutiveFailures=%d, lastFailure=%s ago. " +
                            "Will retry after %s. Last error: %s",
                    cacheKey.tenantId(),
                    state.consecutiveFailures.get(),
                    Duration.between(state.lastFailureTime, Instant.now()),
                    Duration.between(Instant.now(), state.resetTime),
                    state.lastErrorMessage);
            LOGGER.error(message);
            throw CassandraAuthenticationException.Code.TOO_MANY_ATTEMPT_UNSUCCESSFUL_ATTEMPT.get(
                    Map.of("max_consecutive_failures", String.valueOf(MAX_CONSECUTIVE_FAILURES)));
        }
    }

    /**
     * Record a successful session load.
     */
    private void recordSuccess(CQLSessionCacheKey cacheKey) {
        CircuitBreakerState removed = circuitBreakers.remove(cacheKey);
        if (removed != null) {
            LOGGER.info(
                    "Circuit breaker reset after successful load for key={}, previousFailures={}",
                    cacheKey,
                    removed.consecutiveFailures.get());
        }
    }

    /**
     * Record a failed session load attempt.
     */
    private void recordFailure(CQLSessionCacheKey cacheKey, Exception error) {
        CircuitBreakerState state = circuitBreakers.computeIfAbsent(
                cacheKey, k -> new CircuitBreakerState());

        int failures = state.consecutiveFailures.incrementAndGet();
        state.lastFailureTime = Instant.now();
        state.lastErrorMessage = error.getMessage();

        if (failures >= MAX_CONSECUTIVE_FAILURES) {
            state.resetTime = Instant.now().plus(CIRCUIT_BREAKER_RESET_TIMEOUT);
            LOGGER.error(
                    "Circuit breaker OPENED for key={}, consecutiveFailures={}, willResetAt={}",
                    cacheKey,
                    failures,
                    state.resetTime);
        } else {
            LOGGER.warn(
                    "Session load failure recorded for key={}, consecutiveFailures={}/{}",
                    cacheKey,
                    failures,
                    MAX_CONSECUTIVE_FAILURES);
        }
    }

    /**
     * Reset circuit breaker if it exists (called on successful cache hit).
     */
    private void resetCircuitBreakerIfNeeded(CQLSessionCacheKey cacheKey) {
        CircuitBreakerState removed = circuitBreakers.remove(cacheKey);
        if (removed != null && removed.isOpen()) {
            LOGGER.info(
                    "Circuit breaker reset due to successful cache hit for key={}", cacheKey);
        }
    }

    @VisibleForTesting
    protected Optional<CqlSession> peekSession(String tenantId, String authToken, String userAgent) {
        var cacheKey = createCacheKey(tenantId, authToken, userAgent);
        return Optional.ofNullable(sessionCache.getIfPresent(cacheKey))
                .map(SessionValueHolder::session);
    }

    private void onKeyRemoved(
            CQLSessionCacheKey cacheKey, SessionValueHolder sessionHolder, RemovalCause cause) {

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

        if (sessionHolder != null) {
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

    private SessionValueHolder onLoadSession(CQLSessionCacheKey cacheKey) {
        // This is called by Caffeine's loading mechanism
        // The circuit breaker check and locking is done in getSession() before this is called

        var holder =
                new SessionValueHolder(
                        sessionFactory.apply(cacheKey.tenantId(), cacheKey.credentials()), cacheKey);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Loaded CQLSession into cache, holder={}", holder);
        }
        return holder;
    }

    private CQLSessionCacheKey createCacheKey(String tenantId, String authToken, String userAgent) {
        var credentials = credentialsFactory.apply(authToken);
        if (credentials == null) {
            throw new IllegalStateException("credentialsFactory returned null");
        }

        var keyTTL =
                slaUserAgent == null || !slaUserAgent.equalsIgnoreCase(userAgent) ? cacheTTL : slaUserTTL;

        return switch (databaseType) {
            case CASSANDRA, OFFLINE_WRITER -> new CQLSessionCacheKey(
                    tenantId == null || tenantId.isBlank() ? DEFAULT_TENANT : tenantId,
                    credentials,
                    keyTTL,
                    userAgent);
            case ASTRA -> new CQLSessionCacheKey(tenantId, credentials, keyTTL, userAgent);
        };
    }

    @VisibleForTesting
    void clearCache() {
        LOGGER.info("Manually clearing CQLSession cache");
        sessionCache.invalidateAll();
        sessionCache.cleanUp();
        circuitBreakers.clear();
    }

    @VisibleForTesting
    void cleanUp() {
        sessionCache.cleanUp();
    }

    /**
     * For testing: check if circuit breaker is open for a key.
     */
    @VisibleForTesting
    boolean isCircuitBreakerOpen(String tenantId, String authToken, String userAgent) {
        var cacheKey = createCacheKey(tenantId, authToken, userAgent);
        CircuitBreakerState state = circuitBreakers.get(cacheKey);
        return state != null && state.isOpen();
    }

    /**
     * Circuit breaker state for a specific cache key.
     */
    private static class CircuitBreakerState {

        /**
         * tracking failures
         */
        final AtomicInteger consecutiveFailures = new AtomicInteger(0);

        /**
         * Keep tracking for last time we got the error
         */
        Instant lastFailureTime = Instant.now();

        /**
         * Reset Timer after cache hit and timeout
         */
        Instant resetTime = null;

        /**
         * Keep Error Message
         */
        String lastErrorMessage = null;

        /**
         * Circuit is open and we should stop trying.
         *
         * @return
         *      have we reached max MAX_CONSECUTIVE_FAILURES
         */
        private boolean isOpen() {
            return consecutiveFailures.get() >= MAX_CONSECUTIVE_FAILURES && !shouldReset();
        }

        /**
         * Time reached, reset of needed.
         */
        private boolean shouldReset() {
            return resetTime != null && Instant.now().isAfter(resetTime);
        }
    }



    record SessionValueHolder(CqlSession session, CQLSessionCacheKey loadingKey) {
        SessionValueHolder {
            Objects.requireNonNull(session, "session must not be null");
            Objects.requireNonNull(loadingKey, "loadingKey must not be null");
        }

        @Override
        public String toString() {
            return new StringBuilder("SessionValueHolder{")
                    .append("identityHashCode=").append(System.identityHashCode(this))
                    .append(", loadingKey=").append(loadingKey)
                    .append('}').toString();
        }
    }

    static class SessionExpiry implements Expiry<CQLSessionCacheKey, SessionValueHolder> {
        private static final Logger LOGGER = LoggerFactory.getLogger(SessionExpiry.class);

        @Override
        public long expireAfterCreate(CQLSessionCacheKey key, SessionValueHolder value, long currentTime) {
            return value.loadingKey().ttl().toNanos();
        }

        @Override
        public long expireAfterUpdate(
                CQLSessionCacheKey key, SessionValueHolder value, long currentTime, long currentDuration) {
            return currentDuration;
        }

        @Override
        public long expireAfterRead(
                CQLSessionCacheKey key, SessionValueHolder value, long currentTime, long currentDuration) {
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
