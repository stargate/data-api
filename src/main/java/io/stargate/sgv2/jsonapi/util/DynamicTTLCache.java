package io.stargate.sgv2.jsonapi.util;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlCredentials;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
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
 * <p>The {@link DynamicTTLCacheListener} interface will be called when a session is removed from
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
public abstract class DynamicTTLCache<KeyT extends DynamicTTLCache.CacheKey, ValueT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicTTLCache.class);

  private final String cacheName;

  private final AsyncLoadingCache<KeyT, ValueHolder<KeyT, ValueT>> cache;
  private final ValueFactory<KeyT, ValueT> valueFactory;
  private final List<DynamicTTLCacheListener<KeyT, ValueT>> listeners;

  /**
   * Constructs a new instance of the {@link DynamicTTLCache}.
   *
   * <p>Use this overload in production code, see other for detailed description of the parameters.
   */
  protected DynamicTTLCache(
      String cacheName,
      long cacheMaxSize,
      ValueFactory<KeyT, ValueT> valueFactory,
      List<DynamicTTLCacheListener<KeyT, ValueT>> listeners,
      MeterRegistry meterRegistry) {
    this(cacheName, cacheMaxSize, valueFactory, listeners, meterRegistry, false, null);
  }

  /**
   * Constructs a new instance of the {@link DynamicTTLCache}.
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
  protected DynamicTTLCache(
      String cacheName,
      long cacheMaxSize,
      ValueFactory<KeyT, ValueT> valueFactory,
      List<DynamicTTLCacheListener<KeyT, ValueT>> listeners,
      MeterRegistry meterRegistry,
      boolean asyncTaskOnCaller,
      Ticker cacheTicker) {

    this.cacheName = Objects.requireNonNull(cacheName, "cacheName must not be null");
    if (this.cacheName.isBlank()) {
      throw new IllegalArgumentException("cacheName must not be blank");
    }

    this.valueFactory = Objects.requireNonNull(valueFactory, "valueFactory must not be null");
    this.listeners = listeners == null ? List.of() : List.copyOf(listeners);

    LOGGER.info(
        "Initializing DynamicTTLCache with cacheName={}, cacheMaxSize={}, deactivatedTenantConsumers.count={}",
        cacheName,
        cacheMaxSize,
        listeners.size());

    var builder =
        Caffeine.newBuilder()
            .expireAfter(new DynamicExpiryPolicy<KeyT, ValueT>())
            .maximumSize(cacheMaxSize)
            .removalListener(this::onKeyRemoved)
            .recordStats();

    if (asyncTaskOnCaller) {
      LOGGER.warn(
          "Cache {} - CONFIGURED TO RUN ASYNC TASKS SUCH AS CALLBACKS ON THE CALLER THREAD, DO NOT USE IN PRODUCTION.",
          this.cacheName);
      builder = builder.executor(Runnable::run);
    }
    if (cacheTicker != null) {
      LOGGER.warn(
          "Cache {} - CONFIGURED TO USE A CUSTOM TICKER, DO NOT USE IN PRODUCTION.",
          this.cacheName);
      builder = builder.ticker(cacheTicker);
    }
    AsyncLoadingCache<KeyT, ValueHolder<KeyT, ValueT>> loadingCache =
        builder.buildAsync(this::onLoadValue);

    this.cache = CaffeineCacheMetrics.monitor(meterRegistry, loadingCache, this.cacheName);
  }

  /**
   * Asynchronously retrieves or creates value for the given key.
   *
   * <p>Errors from creating the value will be propagated through the Uni.
   */
  protected Uni<ValueT> get(KeyT key) {

    Objects.requireNonNull(key, "key must not be null");

    if (key.forceRefresh()) {
      return Uni.createFrom()
          .completionStage(onLoadValue(key, Runnable::run))
          .invoke(
              (valueHolder) -> {
                cache.put(key, CompletableFuture.completedFuture(valueHolder));
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Force loaded value into cache, holder={}", valueHolder);
                }
              })
          .map(ValueHolder::value);
    }

    return Uni.createFrom().completionStage(cache.get(key)).map(ValueHolder::value);
  }

  @VisibleForTesting
  protected Optional<ValueT> getIfPresent(KeyT key) {
    var future = cache.getIfPresent(key);
    if (future == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(future.join()).map(ValueHolder::value);
  }

  protected boolean evict(KeyT key) {
    // Invalidate the key. This will trigger the onKeyRemoved listener,
    // which will close the CqlSession and run other cleanup.
    // Use .asMap().remove() as the author suggested:
    // https://stackoverflow.com/questions/67994799/how-do-i-make-invalidate-an-entry-and-return-its-value-from-a-caffeine-cache
    boolean entryFound = cache.asMap().remove(key) != null;
    LOGGER.warn(
        "Explicitly evicted session from cache. Cache Key: {} (entry found: {})", key, entryFound);
    return entryFound;
  }

  protected void evictIf(Predicate<KeyT> predicate) {
    cache.synchronous().asMap().keySet().removeIf(predicate);
  }

  /**
   * Process a key being removed from the cache for any reason.
   *
   * <p>
   */
  private void onKeyRemoved(KeyT key, ValueHolder<KeyT, ValueT> valueHolder, RemovalCause cause) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("onKeyRemoved for valueHolder={}, cause={}", valueHolder, cause);
    }

    listeners.forEach(
        consumer -> {
          try {
            consumer.onRemoved(key, valueHolder.value, cause);
          } catch (Exception e) {
            LOGGER.warn(
                "Error calling removal listener: valueHolder={}, cause={}, consumer.class={}",
                valueHolder,
                cause,
                classSimpleName(consumer.getClass()),
                e);
          }
        });
  }

  /**
   * Called to load a new value for the cache, either from caffine because of cache miss or when
   * force refresh is used.
   *
   * <p>
   */
  private CompletableFuture<ValueHolder<KeyT, ValueT>> onLoadValue(KeyT key, Executor executor) {

    // let errors from the factory when it is creating the completion stage propagate out for fast
    // failure.
    CompletionStage<ValueT> stage = valueFactory.apply(key);
    // sanity checking the valueFactory returns a CompletionStage
    if (stage == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("valueFactory returned null CompletionStage for key: " + key));
    }

    return stage
        // if completed exceptionally, it is wrapped in a CompletionStage that is also completed
        // exceptionally
        .exceptionallyCompose(CompletableFuture::failedFuture)
        .thenApply(
            value -> {
              if (value == null) {
                // sanity check
                throw new IllegalStateException("valueFactory returned null for key: " + key);
              }
              var holder = new ValueHolder<>(value, key);
              if (LOGGER.isTraceEnabled()) {
                // so we get the identity hash code of the session holder
                LOGGER.trace("Loaded value into cache, holder={}", holder);
              }
              return holder;
            })
        .toCompletableFuture();
  }

  /**
   * Invalidate all entries and cleanup, for testing when items are invalidated.
   *
   * <p>Note: Removal cause will be {@link RemovalCause#EXPLICIT} for items.
   */
  @VisibleForTesting
  public void clearCache() {
    LOGGER.info("Manually clearing cache");
    cache.synchronous().invalidateAll();
    cache.synchronous().cleanUp();
  }

  /**
   * Clean up the cache, for testing when items are invalidated. The cache will try to be lazy, so
   * things like evictions may not happen exactly at the TTL, this is a way to force it.
   *
   * <p>Look at using a custom ticker (see constructor) to have more control over time in tests.
   */
  @VisibleForTesting
  public void cleanUp() {
    cache.synchronous().cleanUp();
  }

  /**
   * Base for all keys in the cache, which must all have a TTL associated with them.
   *
   * <p>
   */
  public interface CacheKey {
    Duration ttl();

    /**
     * When true the cache will force a refresh of the value even if it is present.
     *
     * <p>Useful or things like the schema cache, where we want to force reload the schema before
     * running schema based commands.
     */
    default boolean forceRefresh() {
      return false;
    }
  }

  /**
   * Holder for the value added to the cache to make it very clear what key was used when it was
   * added so we can get the TTL used when it was loaded.
   *
   * <p>See {@link DynamicExpiryPolicy} for usage.
   */
  record ValueHolder<KeyT extends CacheKey, ValueT>(ValueT value, KeyT loadingKey) {

    ValueHolder {
      Objects.requireNonNull(value, "value must not be null");
      Objects.requireNonNull(loadingKey, "loadingKey must not be null");
    }

    /**
     * Note that the cache can decide when it wants to actually remove an expired key, so we may be
     * closing a session for a tenant at the same time we are opening one. The {@link #toString()}
     * includes the identity hash code of the holder to help with debugging.
     */
    @Override
    public String toString() {
      return new StringBuilder("ValueHolder{")
          .append("identityHashCode=")
          .append(System.identityHashCode(this))
          .append(", loadingKey=")
          .append(loadingKey)
          .append('}')
          .toString();
    }
  }

  /**
   * Dynamic cache Expiry policy that will change the TTL based on the user agent used accessing it.
   *
   * <p>We use the maximum TTL between either the key that was used the load the cache value, or the
   * current key being used to access it. The TTL is set when the key is created based on the user
   * agent coming in. So if an SLA user agent adds it, then a non SLA uses it the non SLA user agent
   * TTL will be used.
   *
   * <p>The last user who access the session will set the TTL for the value if their TTL is higher.
   */
  static class DynamicExpiryPolicy<KeyT extends CacheKey, ValueT>
      implements Expiry<KeyT, ValueHolder<KeyT, ValueT>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicExpiryPolicy.class);

    @Override
    public long expireAfterCreate(KeyT key, ValueHolder<KeyT, ValueT> value, long currentTime) {
      return value.loadingKey().ttl().toNanos();
    }

    @Override
    public long expireAfterUpdate(
        KeyT key, ValueHolder<KeyT, ValueT> value, long currentTime, long currentDuration) {
      return currentDuration;
    }

    @Override
    public long expireAfterRead(
        KeyT key, ValueHolder<KeyT, ValueT> value, long currentTime, long currentDuration) {

      long accessTTL = key.ttl().toNanos();
      long loadTTL = value.loadingKey().ttl().toNanos();

      var newExpiryNano = Math.max(accessTTL, loadTTL);
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "expireAfterRead() - newExpiryNano={}, key.ttl={}, key.identityHashCode={}, value.loadingKey.ttl={}, value.loadingKey.identityHashCode={}",
            newExpiryNano,
            key.ttl(),
            System.identityHashCode(key),
            value.loadingKey.ttl(),
            System.identityHashCode(value.loadingKey));
      }
      return newExpiryNano;
    }
  }

  /** Helper the encapsulated the logic to decide the TTL based on user agent. */
  protected static class DynamicTTLSupplier {

    private final Duration cacheTTL;
    private final UserAgent slaUserAgent;
    private final Duration slaUserTTL;

    /**
     * Constructor for the DynamicTTLSupplier.
     *
     * @param cacheTTL The TTL to use for normal access.
     * @param slaUserAgent The user agent string that identifies SLA users, can be null or empty if
     *     not used.
     * @param slaUserTTL The TTL to use for SLA users, must be non-null and positive if slaUserAgent
     *     is set.
     */
    public DynamicTTLSupplier(Duration cacheTTL, UserAgent slaUserAgent, Duration slaUserTTL) {

      this.cacheTTL = Objects.requireNonNull(cacheTTL, "cacheTTL must not be null");
      if (cacheTTL.isNegative() || cacheTTL.isZero()) {
        throw new IllegalArgumentException("cacheTTL must be positive, was: " + cacheTTL);
      }

      this.slaUserAgent = slaUserAgent;
      if (slaUserAgent != null) {
        this.slaUserTTL =
            Objects.requireNonNull(
                slaUserTTL, "slaUserTTL must not be null is slaUserAgent is set");
        if (slaUserTTL.isNegative() || slaUserTTL.isZero()) {
          throw new IllegalArgumentException("slaUserTTL must be positive");
        }
      } else {
        this.slaUserTTL = null;
      }
    }

    public Duration ttlForUsageAgent(UserAgent userAgent) {
      // slaUserAgent can be null
      return slaUserAgent == null || !slaUserAgent.equals(userAgent) ? cacheTTL : slaUserTTL;
    }

    @Override
    public String toString() {
      return new StringBuilder("DynamicTTLSupplier{")
          .append("cacheTTL=")
          .append(cacheTTL)
          .append(", slaUserAgent=")
          .append(slaUserAgent)
          .append(", slaUserTTL=")
          .append(slaUserTTL)
          .append('}')
          .toString();
    }
  }

  /** Function called when a value is removed from the cache. */
  @FunctionalInterface
  public interface DynamicTTLCacheListener<KeyT extends CacheKey, ValueT> {
    void onRemoved(KeyT key, ValueT value, RemovalCause cause);
  }

  /** Function called to create a new value for the cache when none is needed. */
  @FunctionalInterface
  public interface ValueFactory<KeyT extends CacheKey, ValueT>
      extends Function<KeyT, CompletionStage<ValueT>> {
    CompletionStage<ValueT> apply(KeyT key);
  }
}
