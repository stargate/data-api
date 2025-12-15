package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link DynamicTTLCache}. */
public class DynamicTTLCacheTests extends CacheTestsBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicTTLCacheTests.class);

  private final String KEY = "key-" + TEST_CONSTANTS.CORRELATION_ID;
  private final String VALUE = "value-" + TEST_CONSTANTS.CORRELATION_ID;
  private final TestDynamicTTLCache.TestKey CACHE_KEY =
      new TestDynamicTTLCache.TestKey(KEY, LONG_TTL);

  @Test
  public void cacheStartsEmtpy() {

    var fixture = newFixture();

    // cache size is not reliable, peeking to see if our session is present
    assertThat(fixture.cache.peekValue(CACHE_KEY)).as("Cache is empty when started").isNotPresent();
  }

  @Test
  public void valueFactoryCalledOnceOnly() {

    var fixture = newFixture();

    var actualValue = fixture.cache.getValue(CACHE_KEY);
    assertThat(actualValue)
        .as("Value from cache is instance from factory")
        .isSameAs(fixture.expectedValue);

    // Value factory called with the key
    verify(fixture.valueFactory()).apply(CACHE_KEY);

    verifyNoMoreInteractions(fixture.valueFactory);

    // Clear invocation counts, stubb remains
    clearInvocations(fixture.valueFactory);

    // Second call to check cache hit, not ticker / clock as not moved
    var actualValue2 = fixture.cache.getValue(CACHE_KEY);
    assertThat(actualValue2)
        .as("Value from cache is instance from factory")
        .isSameAs(fixture.expectedValue);
    assertThat(actualValue2).as("Value from second call is same as first").isSameAs(actualValue);

    // Verify that session factory is not called again
    verifyNoInteractions(fixture.valueFactory);
  }

  @Test
  public void limitedToMaxSize() {

    var fixture = newFixture();

    for (int i = 0; i < CACHE_MAX_SIZE + 1; i++) {

      var key = thisCacheKey(i, LONG_TTL);
      var expectedValue = thisValue(i);

      when(fixture.valueFactory.apply(key))
          .thenReturn(CompletableFuture.completedFuture(expectedValue));

      var actualSession = fixture.cache.getValue(key);
      assertThat(actualSession)
          .as("Value from cache is instance from factory")
          .isSameAs(expectedValue);
    }

    fixture.cache.cleanUp();

    // listener called with key-0 because the size of the cache was exceeded
    verify(fixture.listener).onRemoved(eq(thisCacheKey(0, LONG_TTL)), any(), any());
    verifyNoMoreInteractions(fixture.listener);

    // key 0 should not be in the cache
    assertThat(fixture.cache.peekValue(thisCacheKey(0, LONG_TTL)))
        .as("Key 0 is not in cache after adding more than max size")
        .isNotPresent();

    for (int i = 1; i < CACHE_MAX_SIZE + 1; i++) {

      assertThat(fixture.cache.peekValue(thisCacheKey(i, LONG_TTL)))
          .as("Key `%s` is in cache after adding more than max size", thisKey(i))
          .isPresent();
    }
  }

  @Test
  public void listenerCalledOnForcedEviction() {

    var fixture = newFixture();

    var actualValue = fixture.cache.getValue(CACHE_KEY);

    ///  not testing the timeing, just that the listener is called
    // so brute force the eviction is OK
    fixture.cache.clearCache();

    // should have called our listener
    verify(fixture.listener).onRemoved(eq(CACHE_KEY), eq(actualValue), eq(RemovalCause.EXPLICIT));

    assertThat(fixture.cache.peekValue(CACHE_KEY))
        .as("Value is not in cache after forced eviction")
        .isNotPresent();
  }

  @Test
  public void listenerCalledOnExpiredEviction() {

    var fixture = newFixture();

    var actualValue = fixture.cache.getValue(CACHE_KEY);

    // must not call consumer until evicted
    verifyNoInteractions(fixture.listener);

    // fake time moving
    fixture.ticker.advance(LONG_TTL.plus(LONG_TTL));
    fixture.cache.cleanUp();

    // should have called our listener
    verify(fixture.listener).onRemoved(eq(CACHE_KEY), eq(actualValue), eq(RemovalCause.EXPIRED));

    assertThat(fixture.cache.peekValue(CACHE_KEY))
        .as("Session is not in cache after expired eviction")
        .isNotPresent();
  }

  @Test
  public void listenerErrorSwallowed() {

    var consumer1 = listenerWithLogging();
    doThrow(new RuntimeException("test exception")).when(consumer1).onRemoved(any(), any(), any());
    var consumer2 = listenerWithLogging();

    // get a session added to the cache, then evict it, so the callbacks are run
    var fixture = newFixture(List.of(consumer1, consumer2));

    var actualValue = fixture.cache.getValue(CACHE_KEY);

    // force eviction to get callbacks, not fussed on the reason for eviction
    fixture.cache.clearCache();
    // should call both consumers, even though consumer 1 throws an exception
    verify(consumer1).onRemoved(eq(CACHE_KEY), eq(actualValue), any());
    verify(consumer2).onRemoved(eq(CACHE_KEY), eq(actualValue), any());
    verifyNoMoreInteractions(consumer1, consumer2);
  }

  @Test
  public void expireAfterReadPicksHighestTTL() {

    var expiry = new DynamicTTLCache.DynamicExpiryPolicy<TestDynamicTTLCache.TestKey, String>();
    var fixture = newFixture();

    var longKey = thisCacheKey(0, LONG_TTL);
    var shortKey = thisCacheKey(0, SHORT_TTL);

    // Value initially loaded with the short TTL
    var valueHolderLoadedShort = new DynamicTTLCache.ValueHolder<>(fixture.expectedValue, shortKey);
    var nanosLowToHigh = expiry.expireAfterRead(longKey, valueHolderLoadedShort, 0, 0);
    assertThat(nanosLowToHigh)
        .as("Loaded with short TTL, access with long TTL access, new TTL is long")
        .isEqualTo(longKey.ttl.toNanos());

    var nanosLowToLow = expiry.expireAfterRead(shortKey, valueHolderLoadedShort, 0, 0);
    assertThat(nanosLowToLow)
        .as("Loaded with short TTL, access with short TTL access, new TTL is short")
        .isEqualTo(shortKey.ttl.toNanos());

    // Loaded with High TTL
    var valueHolderLoadedLong = new DynamicTTLCache.ValueHolder<>(fixture.expectedValue, longKey);
    var nanosHighToLow = expiry.expireAfterRead(shortKey, valueHolderLoadedLong, 0, 0);
    assertThat(nanosHighToLow)
        .as("Loaded with long TTL, access with short TTL, new TTL remains long")
        .isEqualTo(longKey.ttl.toNanos());

    var nanosHighToHigh = expiry.expireAfterRead(longKey, valueHolderLoadedLong, 0, 0);
    assertThat(nanosHighToHigh)
        .as("Loaded with long TTL, access with long TTL, new TTL remains long")
        .isEqualTo(longKey.ttl.toNanos());
  }

  @Test
  public void expireAfterCreateUsesLoadingKey() {
    var expiry = new DynamicTTLCache.DynamicExpiryPolicy<TestDynamicTTLCache.TestKey, String>();
    var fixture = newFixture();

    var longKey = thisCacheKey(0, LONG_TTL);
    var shortKey = thisCacheKey(0, SHORT_TTL);

    var valueHolder = new DynamicTTLCache.ValueHolder<>(fixture.expectedValue, longKey);

    // actual call will pass the loading key for the first param,
    // as a test I want to make sure it uses the key on the value holder
    var actualNanos = expiry.expireAfterCreate(shortKey, valueHolder, 0);
    assertThat(actualNanos)
        .as("Expire after create is from the loading key on value holder")
        .isEqualTo(longKey.ttl.toNanos());
  }

  @Test
  public void expireShortTTL() {

    var fixture = newFixture();
    var longKey = thisCacheKey(0, LONG_TTL);
    var shortKey = thisCacheKey(0, SHORT_TTL);

    // request a value using a key with a short TTL
    var actualValue = fixture.cache.getValue(shortKey);

    assertThat(fixture.cache.peekValue(shortKey))
        .as("Value is present in cache when peeking - before expiry")
        .isPresent();

    // advance the time past the Short TTL, and give the cache time to clean up
    fixture.ticker.advance(SHORT_TTL.plusNanos(10));
    fixture.cache.cleanUp();

    assertThat(fixture.cache.peekValue(shortKey))
        .as("Value is not present in cache when peeking - after expiry")
        .isNotPresent();
  }

  @Test
  public void expireKeepsSessionAfterNonSLAUserRead() {

    var fixture = newFixture();

    var longKey = thisCacheKey(0, LONG_TTL);
    var shortKey = thisCacheKey(0, SHORT_TTL);

    // request a value using the Short TTL
    var actualValue = fixture.cache.getValue(shortKey);

    assertThat(actualValue)
        .as("Value from cache is instance from factory")
        .isSameAs(fixture.expectedValue);

    assertThat(fixture.cache.peekValue(shortKey))
        .as("Value is present in cache when peeking - before expiry")
        .isPresent();

    // Now get the value with the Long TTL, beyond the Short TTL
    var actualValue2 = fixture.cache.getValue(longKey);

    // advance the time past the Short TTL, and give the cache time to clean up
    fixture.ticker.advance(SHORT_TTL.plusNanos(10));
    fixture.cache.cleanUp();

    // the Value should still be present, using both the Short and Long TTL keys
    assertThat(fixture.cache.peekValue(shortKey))
        .as("Value is present in cache when peeking with Short TTL - after Short TTL expiry")
        .isPresent();
    assertThat(fixture.cache.peekValue(longKey))
        .as("Value is present in cache when peeking with Long TTL - after Short TTL expiry")
        .isPresent();

    // advance the time past the Long TTL, and give the cache time to clean up
    fixture.ticker.advance(LONG_TTL.plusNanos(10));
    fixture.cache.cleanUp();

    // the Value should not be present, using both the Short and Long TTL keys
    assertThat(fixture.cache.peekValue(shortKey))
        .as("Value is not present in cache when peeking with Short TTL - after Long TTL expiry")
        .isNotPresent();
    assertThat(fixture.cache.peekValue(longKey))
        .as("Value is not present in cache when peeking with Long TTL - after Long TTL expiry")
        .isNotPresent();
  }

  // =======================================================
  // Metrics related tests
  // =======================================================
  @Test
  public void metricsHitsAndMisses() {
    var fixture = newFixture();

    // 1. Miss + Load
    fixture.cache.getValue(CACHE_KEY);
    fixture.cache.cleanUp();

    assertThat(fixture.meterRegistry.get("cache.gets").tag("result", "miss").counter().count())
        .as("Cache miss count")
        .isEqualTo(1.0);
    assertThat(fixture.meterRegistry.get("cache.gets").tag("result", "hit").counter().count())
        .as("Cache hit count")
        .isEqualTo(0.0);

    // Check loads
    assertThat(fixture.meterRegistry.get("cache.loads").tag("result", "success").timer().count())
        .as("Cache load success count")
        .isEqualTo(1);

    // 2. Hit
    fixture.cache.getValue(CACHE_KEY);
    fixture.cache.cleanUp();

    assertThat(fixture.meterRegistry.get("cache.gets").tag("result", "miss").counter().count())
        .as("Cache miss count")
        .isEqualTo(1.0);
    assertThat(fixture.meterRegistry.get("cache.gets").tag("result", "hit").counter().count())
        .as("Cache hit count")
        .isEqualTo(1.0);

    assertThat(fixture.meterRegistry.get("cache.loads").tag("result", "success").timer().count())
        .as("Cache load success count")
        .isEqualTo(1); // Load count should not increase
  }

  @Test
  public void metricsSize() {
    var fixture = newFixture();

    // Initial size should be 0
    var cacheSizeMetric = fixture.meterRegistry.find("cache.size").tag("cache", CACHE_NAME).gauge();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).as("Initial cache size is 0").isEqualTo(0.0);

    // Add first entry
    fixture.cache.getValue(thisCacheKey(0, LONG_TTL));
    fixture.cache.cleanUp();

    assertThat(cacheSizeMetric.value())
        .as("Cache size is 1 after adding first entry")
        .isEqualTo(1.0);

    // Add second entry
    fixture.cache.getValue(thisCacheKey(1, LONG_TTL));
    fixture.cache.cleanUp();

    assertThat(cacheSizeMetric.value())
        .as("Cache size is 2 after adding second entry")
        .isEqualTo(2.0);

    // Explicitly evict
    fixture.cache.clearCache();

    assertThat(cacheSizeMetric.value()).as("Cache size is 0 after eviction").isEqualTo(0.0);
  }

  @Test
  public void metricsCacheLoadFailure() {
    var fixture = newFixture();

    // Create a key that will cause the value factory to fail
    var failingKey = new TestDynamicTTLCache.TestKey("FAIL-" + KEY, LONG_TTL);
    when(fixture.valueFactory.apply(failingKey))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Load failed")));

    // Attempt to load value - should fail and assert the loading failure happened
    Throwable thrown = catchThrowable(() -> fixture.cache.getValue(failingKey));
    assertThat(thrown)
        .as("Should throw exception on load failure")
        .hasMessageContaining("Load failed");

    fixture.cache.cleanUp();

    var loadFailureTimer =
        fixture
            .meterRegistry
            .find("cache.loads")
            .tag("cache", CACHE_NAME)
            .tag("result", "failure")
            .timer();
    assertThat(loadFailureTimer).as("cache.loads failure timer exists").isNotNull();
    assertThat(loadFailureTimer.count())
        .as("Load failure count is at least 1")
        .isGreaterThanOrEqualTo(1L);
  }

  @Test
  public void metricsEvictionsExplicit() {
    var fixture = newFixture();

    int count = 3;

    // Add multiple items
    for (int i = 0; i < count; i++) {
      fixture.cache.getValue(thisCacheKey(i, LONG_TTL));
    }

    // Evict them explicitly
    for (int i = 0; i < count; i++) {
      fixture.cache.evict(thisCacheKey(i, LONG_TTL));
    }

    fixture.cache.cleanUp();

    assertThat(
            fixture.meterRegistry.get("cache.evictions").tag("cause", "EXPLICIT").summary().count())
        .as("Explicit eviction count should match number of evictions")
        .isEqualTo(count);

    assertThat(fixture.meterRegistry.get("cache.size").gauge().value())
        .as("Cache size after all evictions")
        .isEqualTo(0.0);
  }

  @Test
  public void metricsEvictionsNotRecordedWhenKeyNotInCache() {
    var fixture = newFixture();

    // Ensure key is not in cache
    assertThat(fixture.cache.peekValue(CACHE_KEY))
        .as("Cache is empty before eviction")
        .isNotPresent();

    // Try to evict
    boolean evicted = fixture.cache.evict(CACHE_KEY);
    assertThat(evicted).as("Eviction should return false when key not present").isFalse();

    // Verify cache is still empty
    assertThat(fixture.cache.peekValue(CACHE_KEY))
        .as("Cache is still empty after eviction")
        .isNotPresent();

    fixture.cache.cleanUp();

    // The `onKeyRemoved` method will not be triggered if the key is not in the cache - no
    // actual eviction
    var evictionMetric =
        fixture
            .meterRegistry
            .find("cache.evictions")
            .tag("cache", CACHE_NAME)
            .tag("cause", "EXPLICIT")
            .summary();
    assertThat(evictionMetric).as("Explicit eviction metric should be present").isNotNull();
    assertThat(evictionMetric.count())
        .as("No metric should not be recorded in registry if no actual eviction happened")
        .isEqualTo(0);
  }

  @Test
  public void metricsEvictionsSize() {
    var fixture = newFixture();

    // Add 11 items (Max size is 10)
    for (int i = 0; i < CACHE_MAX_SIZE + 1; i++) {
      fixture.cache.getValue(thisCacheKey(i, LONG_TTL));
    }
    fixture.cache.cleanUp();

    // The oldest one (0) should be evicted
    assertThat(fixture.cache.peekValue(thisCacheKey(0, LONG_TTL))).isNotPresent();

    assertThat(fixture.meterRegistry.get("cache.evictions").tag("cause", "SIZE").summary().count())
        .as("Size eviction count")
        .isEqualTo(1);
  }

  @Test
  public void metricsEvictionsExpired() {
    var fixture = newFixture();
    fixture.cache.getValue(CACHE_KEY);

    // Advance time to expire
    fixture.ticker.advance(LONG_TTL.plusNanos(1));
    fixture.cache.cleanUp();

    assertThat(
            fixture.meterRegistry.get("cache.evictions").tag("cause", "EXPIRED").summary().count())
        .as("Expired eviction count")
        .isEqualTo(1);
  }

  @Test
  public void metricsEvictionsReplaced() {
    var fixture = newFixture();
    fixture.cache.getValue(CACHE_KEY);

    // Force refresh to trigger replacement
    // Same key string, so it maps to same entry, but forceRefresh=true triggers put()
    var forceRefreshKey = new TestDynamicTTLCache.TestKey(KEY, LONG_TTL, true);
    fixture.cache.getValue(forceRefreshKey);
    fixture.cache.cleanUp();

    assertThat(
            fixture.meterRegistry.get("cache.evictions").tag("cause", "REPLACED").summary().count())
        .as("Replaced eviction count")
        .isEqualTo(1);
  }

  // =======================================================
  // Helpers / no more tests below
  // =======================================================

  TestDynamicTTLCache.TestKey thisCacheKey(int i, Duration ttl) {
    return new TestDynamicTTLCache.TestKey(thisKey(i), ttl);
  }

  private String thisKey(int i) {
    return "%s-%s".formatted(KEY, i);
  }

  private String thisValue(int i) {
    return "%s-%s".formatted(VALUE, i);
  }

  private record Fixture(
      TestDynamicTTLCache cache,
      String expectedValue,
      DynamicTTLCache.ValueFactory<TestDynamicTTLCache.TestKey, String> valueFactory,
      DynamicTTLCache.DynamicTTLCacheListener<TestDynamicTTLCache.TestKey, String> listener,
      MeterRegistry meterRegistry,
      FakeTicker ticker) {}

  private Fixture newFixture() {
    return newFixture(null);
  }

  private Fixture newFixture(
      List<DynamicTTLCache.DynamicTTLCacheListener<TestDynamicTTLCache.TestKey, String>>
          listeners) {

    var valueFactory =
        (DynamicTTLCache.ValueFactory<TestDynamicTTLCache.TestKey, String>)
            mock(DynamicTTLCache.ValueFactory.class);

    when(valueFactory.apply(any())).thenReturn(CompletableFuture.completedFuture(VALUE));

    var meterRegistry = new SimpleMeterRegistry();
    var fakeTicker = new FakeTicker();

    DynamicTTLCache.DynamicTTLCacheListener<TestDynamicTTLCache.TestKey, String> listener = null;
    if (listeners == null) {
      listener = listenerWithLogging();
      listeners = List.of(listener);
    }

    // run async on caller thread so they will reliably complete before the test ends, used for
    // removal callback
    var cache =
        new TestDynamicTTLCache(
            CACHE_NAME, CACHE_MAX_SIZE, valueFactory, listeners, meterRegistry, true, fakeTicker);

    return new Fixture(cache, VALUE, valueFactory, listener, meterRegistry, fakeTicker);
  }

  private DynamicTTLCache.DynamicTTLCacheListener<TestDynamicTTLCache.TestKey, String>
      listenerWithLogging() {
    var listener =
        (DynamicTTLCache.DynamicTTLCacheListener<TestDynamicTTLCache.TestKey, String>)
            mock(DynamicTTLCache.DynamicTTLCacheListener.class);

    doAnswer(
            invocation -> {
              var key = invocation.getArgument(0);
              var value = invocation.getArgument(1);
              var cause = invocation.getArgument(2);
              System.out.println(
                  "DynamicTTLCacheListener - onRemoved called with key: "
                      + key
                      + ", value: "
                      + value
                      + ", cause: "
                      + cause);
              return null;
            })
        .when(listener)
        .onRemoved(any(), any(), any());
    return listener;
  }

  static class TestDynamicTTLCache extends DynamicTTLCache<TestDynamicTTLCache.TestKey, String> {

    TestDynamicTTLCache(
        String cacheName,
        long cacheMaxSize,
        ValueFactory<TestDynamicTTLCache.TestKey, String> valueFactory,
        List<DynamicTTLCacheListener<TestDynamicTTLCache.TestKey, String>> listeners,
        MeterRegistry meterRegistry,
        boolean asyncOnCaller,
        Ticker cacheTicker) {
      super(
          cacheName,
          cacheMaxSize,
          valueFactory,
          listeners,
          meterRegistry,
          asyncOnCaller,
          cacheTicker);
    }

    String getValue(TestDynamicTTLCache.TestKey key) {
      // we are just tesing the cache is calling things, there is no networking we can wait
      return get(key).await().indefinitely();
    }

    protected Optional<String> peekValue(TestDynamicTTLCache.TestKey key) {
      return getIfPresent(key);
    }

    record TestKey(String key, Duration ttl, boolean forceRefresh)
        implements DynamicTTLCache.CacheKey {

      TestKey(String key, Duration ttl) {
        this(key, ttl, false);
      }

      // hash and equals on the key only, we dont use the TTL in the equals its only for the expiry
      @Override
      public int hashCode() {
        return key.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (!(obj instanceof TestKey other)) {
          return false;
        }
        return key.equals(other.key);
      }
    }
  }
}
