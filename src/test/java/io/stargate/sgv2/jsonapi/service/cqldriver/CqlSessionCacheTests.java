package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link CQLSessionCache}. */
public class CqlSessionCacheTests {

  private static final String AUTH_TOKEN = "authToken";
  private static final String TENANT_ID = "tenantId";
  private static final Duration CACHE_TTL = Duration.ofSeconds(1);
  private final int CACHE_MAX_SIZE = 10;

  private static final String SLA_USER_AGENT = "SLA_USER_AGENT";
  private static final Duration SLA_USER_TTL = Duration.ofMillis(500);

  @Test
  public void cacheStartsEmtpy() {

    var fixture = newFixture();

    // cache size is not reliable, peeking to see if our session is present
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, null))
        .as("Cache is empty when started")
        .isNotPresent();
  }

  @Test
  public void sessionFactoryCalledOnceOnly() {

    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // auth token was passed to credentials factory
    verify(fixture.credentialsFactory).apply(AUTH_TOKEN);

    // tenant and expected credentials passed to the session factory
    verify(fixture.sessionFactory).apply(TENANT_ID, fixture.expectedCredentials);

    verifyNoMoreInteractions(fixture.credentialsFactory, fixture.sessionFactory);

    // Clear invocation counts, stubb remains
    clearInvocations(fixture.credentialsFactory, fixture.sessionFactory);

    // Second call to check cache hit
    var actualSession2 = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
    assertThat(actualSession2)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);
    assertThat(actualSession2)
        .as("Session from second call is same as first")
        .isSameAs(actualSession);

    // Verify that session factory is not called again
    verifyNoInteractions(fixture.sessionFactory);
  }

  @Test
  public void cassandraCache() {

    // only testing it works with the CASSANDRA db type, all other logic is tested with ASTRA
    var fixture =
        newFixture(
            DatabaseType.CASSANDRA,
            List.of(),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            false,
            true);

    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // auth token was passed to credentials factory
    verify(fixture.credentialsFactory).apply(AUTH_TOKEN);

    // tenant and expected credentials passed to the session factory
    verify(fixture.sessionFactory).apply(TENANT_ID, fixture.expectedCredentials);

    verifyNoMoreInteractions(fixture.credentialsFactory, fixture.sessionFactory);
  }

  @Test
  public void limitedToMaxSize() {

    var consumer = consumerWithLogging();
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(consumer),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            true,
            false);

    for (int i = 0; i < CACHE_MAX_SIZE + 1; i++) {

      // credentials are part of key equality, so we need to generate creds
      // with the same quality here and below when testing with peek
      var expectedCredentials = fixture.setTokenCredentials(thisAuthToken(i));

      var expectedSession = mock(CqlSession.class);
      when(fixture.sessionFactory.apply(thisTenantId(i), expectedCredentials))
          .thenReturn(expectedSession);

      var actualSession = fixture.cache.getSession(thisTenantId(i), thisAuthToken(i), null);
      assertThat(actualSession)
          .as("Session from cache is instance from factory")
          .isSameAs(expectedSession);
    }

    fixture.cache.cleanUp();

    // consumer called with tenantId-0 because the size of the cache was exceeded
    verify(consumer).accept(thisTenantId(0), RemovalCause.SIZE);
    verifyNoMoreInteractions(consumer);

    // tenant 0 should not be in the cache
    fixture.setTokenCredentials(thisAuthToken(0));
    assertThat(fixture.cache.peekSession(thisTenantId(0), thisAuthToken(0), null))
        .as("Tenant 0 session is not in cache after adding more than max size")
        .isNotPresent();

    for (int i = 1; i < CACHE_MAX_SIZE + 1; i++) {

      assertThat(fixture.cache.peekSession(thisTenantId(i), thisAuthToken(i), null))
          .as("Tenant `%s` session is in cache after adding more than max size", thisTenantId(i))
          .isPresent();
    }
  }

  @Test
  public void sessionClosedAndEvictionListenerCalledOnForced() {
    var consumer = consumerWithLogging();
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(consumer),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            false,
            true);

    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    ///  we dont care why the tenant was marked inactive, so do not need to wait for TTL, clear the
    // cache to force eviction
    fixture.cache.clearCache();
    // Session must be closed when evicted
    verify(fixture.expectedSession).close();

    // should have called our consumer when evicted
    verify(consumer).accept(eq(TENANT_ID), any());

    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, null))
        .as("Session is not in cache after eviction")
        .isNotPresent();
  }

  @Test
  public void sessionClosedAndEvictionListenerCalledOnExpired() {

    var consumer = consumerWithLogging();
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(consumer),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            true,
            true);

    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // move the fake time forward to expire the session
    fixture.ticker.advance(CACHE_TTL.plusSeconds(10));

    // let the cache cleanup
    fixture.cache.cleanUp();

    // Session must be closed when expired
    verify(fixture.expectedSession).close();

    // should have called our consumer when expired
    verify(consumer).accept(TENANT_ID, RemovalCause.EXPIRED);

    // session should not be in the cache
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, null))
        .as("Session is not in cache after expried")
        .isNotPresent();
  }

  @Test
  public void evictionListenerNotCalledUntilEvicted() {

    var longTTL = Duration.ofSeconds(5);
    var consumer = mock(CQLSessionCache.DeactivatedTenantConsumer.class);
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(consumer),
            longTTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            false,
            true);

    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);

    // must not call consumer until evicted
    verifyNoInteractions(consumer);

    // force eviction so not waiting for longTTL
    fixture.cache.clearCache();
    verify(consumer).accept(TENANT_ID, RemovalCause.EXPLICIT);
    verifyNoMoreInteractions(consumer);
  }

  @Test
  public void callbackErrorSwallowed() {

    var consumer1 = consumerWithLogging();
    doThrow(new RuntimeException("test exception")).when(consumer1).accept(any(), any());
    var consumer2 = consumerWithLogging();

    // get a session added to the cache, then evict it, so the callbacks are run
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(consumer1, consumer2),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            false,
            true);
    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // force eviction to get callbacks
    fixture.cache.clearCache();
    // should call both consumers, even though consumer 1 throws an exception
    verify(consumer1).accept(TENANT_ID, RemovalCause.EXPLICIT);
    verify(consumer2).accept(TENANT_ID, RemovalCause.EXPLICIT);
    verifyNoMoreInteractions(consumer1, consumer2);
  }

  @Test
  public void nullCredentialsCausesError() {
    var fixture = newFixture();
    when(fixture.credentialsFactory.apply(AUTH_TOKEN)).thenReturn(null);

    assertThrows(
        IllegalStateException.class,
        () -> {
          fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);
        });
  }

  @Test
  public void nullTenantIdToEmptyString() {
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            false,
            false);

    var expectedCredentials = mock(CqlCredentials.class);
    when(fixture.credentialsFactory.apply(AUTH_TOKEN)).thenReturn(expectedCredentials);

    var expectedSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply("", expectedCredentials)).thenReturn(expectedSession);

    var actualSession = fixture.cache.getSession(null, AUTH_TOKEN, null);

    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(expectedSession);

    // session is created with an empty string token
    verify(fixture.sessionFactory).apply("", expectedCredentials);
  }

  @Test
  public void metricsAdded() {
    // only testing they are added to the registry with the expected name, not checking the values
    // that is a feature of the cache itself

    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            false,
            true);
    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);

    // give ethe cache time to bookkeep
    fixture.cache.cleanUp();

    var cacheSizeMetric =
        fixture.meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    assertThat(cacheSizeMetric)
        .as("cache.size metric added to registry with expected name")
        .isNotNull();

    var cachePutMetric =
        fixture
            .meterRegistry
            .find("cache.puts")
            .tag("cache", "cql_sessions_cache")
            .functionCounter();
    assertThat(cachePutMetric)
        .as("cache.puts metric added to registry with expected name")
        .isNotNull();
  }

  @Test
  public void expireAfterReadPicksHighestTTL() {

    var expiry = new CQLSessionCache.SessionExpiry();

    var lowTTL = Duration.ofSeconds(1);
    var highTTL = Duration.ofSeconds(10);

    var lowTTLKey = mock(CQLSessionCache.SessionCacheKey.class);
    when(lowTTLKey.ttl()).thenReturn(lowTTL);

    var highTTLKey = mock(CQLSessionCache.SessionCacheKey.class);
    when(highTTLKey.ttl()).thenReturn(highTTL);

    // Loaded with Low TTL
    var valueHolderLoadedLow = mock(CQLSessionCache.SessionValueHolder.class);
    when(valueHolderLoadedLow.loadingKey()).thenReturn(lowTTLKey);

    var nanosLowToHigh = expiry.expireAfterRead(highTTLKey, valueHolderLoadedLow, 0, 0);
    assertThat(nanosLowToHigh)
        .as("Loaded with low TTL, access with high TTL access, new TTL is high")
        .isEqualTo(highTTL.toNanos());

    var nanosLowToLow = expiry.expireAfterRead(lowTTLKey, valueHolderLoadedLow, 0, 0);
    assertThat(nanosLowToLow)
        .as("Loaded with low TTL, access with low TTL access, new TTL is low")
        .isEqualTo(lowTTL.toNanos());

    // Loaded with High TTL
    var valueHolderLoadedHigh = mock(CQLSessionCache.SessionValueHolder.class);
    when(valueHolderLoadedHigh.loadingKey()).thenReturn(highTTLKey);

    var nanosHighToLow = expiry.expireAfterRead(lowTTLKey, valueHolderLoadedHigh, 0, 0);
    assertThat(nanosHighToLow)
        .as("Loaded with high TTL, access with low TTL, new TTL remains high")
        .isEqualTo(highTTL.toNanos());

    var nanosHighToHigh = expiry.expireAfterRead(highTTLKey, valueHolderLoadedHigh, 0, 0);
    assertThat(nanosHighToHigh)
        .as("Loaded with high TTL, access with high TTL, new TTL remains high")
        .isEqualTo(highTTL.toNanos());
  }

  @Test
  public void expireAfterCreateUsesLoadingKey() {
    var expiry = new CQLSessionCache.SessionExpiry();

    var lowTTL = Duration.ofSeconds(1);
    var highTTL = Duration.ofSeconds(10);

    var lowTTLKey = mock(CQLSessionCache.SessionCacheKey.class);
    when(lowTTLKey.ttl()).thenReturn(lowTTL);

    var highTTLKey = mock(CQLSessionCache.SessionCacheKey.class);
    when(highTTLKey.ttl()).thenReturn(highTTL);

    var valueHolder = mock(CQLSessionCache.SessionValueHolder.class);
    when(valueHolder.loadingKey()).thenReturn(highTTLKey);

    // actual call will pass the loading key for the first param,
    // as a test I want to make sure it uses the key on the value holder
    var actualNanos = expiry.expireAfterCreate(lowTTLKey, valueHolder, 0);
    assertThat(actualNanos)
        .as("Expire after create is from the loading key on value holder")
        .isEqualTo(highTTL.toNanos());
  }

  @Test
  public void expireSlaUser() {

    // using the fake ticker so we can control the time
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            true,
            true);

    // request a session using the SLA user agent, this will be expired after SLA_USER_TTL
    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT);

    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT))
        .as("SLA Session is present in cache when peeking - before expiry")
        .isPresent();

    // advance the time past the TTL for the SLA user, and give the cache time to clean up
    fixture.ticker.advance(SLA_USER_TTL.plusNanos(10));
    fixture.cache.cleanUp();

    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT))
        .as("SLA Session is not present in cache when peeking - after expiry")
        .isNotPresent();
  }

  @Test
  public void expireKeepsSessionAfterNonSLAUserRead() {

    // using the fake ticker so we can control the time
    var fixture =
        newFixture(
            DatabaseType.ASTRA,
            List.of(),
            CACHE_TTL,
            CACHE_MAX_SIZE,
            SLA_USER_AGENT,
            SLA_USER_TTL,
            true,
            true);

    // request a session using the SLA user agent, this will be expired after SLA_USER_TTL
    var actualSession = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT);

    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT))
        .as("SLA Session is present in cache when peeking - before expiry")
        .isPresent();

    // Now get the session with a different user agent, to reset the TTL beyond the SLA user TTL
    var actualSession2 = fixture.cache.getSession(TENANT_ID, AUTH_TOKEN, null);

    // advance the time past the TTL for the SLA user, and give the cache time to clean up
    fixture.ticker.advance(SLA_USER_TTL.plusNanos(10));
    fixture.cache.cleanUp();

    // the SLA session should still be present, using both the SLA and non SLA user agent
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT))
        .as("SLA Session is  present in cache when peeking with SLA agent - after SLA expiry")
        .isPresent();
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, null))
        .as("SLA Session is  present in cache when peeking non SLA agent - after SLA expiry")
        .isPresent();

    // advance the time past the TTL for the non SLA user, and give the cache time to clean up
    fixture.ticker.advance(CACHE_TTL.plusNanos(10));
    fixture.cache.cleanUp();

    // the SLA session should still be REMOVED, using both the SLA and non SLA user agent
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, SLA_USER_AGENT))
        .as(
            "SLA Session is not present in cache when peeking with SLA agent - after non SLA expiry")
        .isNotPresent();
    assertThat(fixture.cache.peekSession(TENANT_ID, AUTH_TOKEN, null))
        .as("SLA Session is not present in cache when peeking non SLA agent - after non SLA expiry")
        .isNotPresent();
  }

  // =======================================================
  // Helpers / no more tests below
  // =======================================================

  private String thisTenantId(int i) {
    return "%s-%s".formatted(TENANT_ID, i);
  }

  private String thisAuthToken(int i) {
    return "%s-%s".formatted(AUTH_TOKEN, i);
  }

  private CQLSessionCache.DeactivatedTenantConsumer consumerWithLogging() {
    var consumer = mock(CQLSessionCache.DeactivatedTenantConsumer.class);
    doAnswer(
            invocation -> {
              var tenantId = invocation.getArgument(0);
              var cause = invocation.getArgument(1);
              System.out.println(
                  "Eviction listener called for tenantId: " + tenantId + ", cause: " + cause);
              return null;
            })
        .when(consumer)
        .accept(any(), any());
    return consumer;
  }

  /** {@link Ticker} for the cache so we can control the time for testing differentiated TTL */
  static class FakeTicker implements Ticker {

    private long nanos = 0;

    @Override
    public long read() {
      return nanos;
    }

    public void advance(Duration duration) {
      nanos += duration.toNanos();
    }
  }

  private record Fixture(
      CQLSessionCache cache,
      CqlCredentials expectedCredentials,
      CqlCredentialsFactory credentialsFactory,
      CqlSession expectedSession,
      CQLSessionCache.SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      FakeTicker ticker) {

    CqlCredentials setTokenCredentials(String authToken) {
      var expectedCredentials = new CqlCredentials.TokenCredentials(authToken);
      when(this.credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);
      return expectedCredentials;
    }
  }

  private Fixture newFixture() {
    return newFixture(
        DatabaseType.ASTRA,
        List.of(),
        CACHE_TTL,
        CACHE_MAX_SIZE,
        SLA_USER_AGENT,
        SLA_USER_TTL,
        false,
        true);
  }

  private Fixture newFixture(
      DatabaseType databaseType,
      List<CQLSessionCache.DeactivatedTenantConsumer> consumers,
      Duration cacheTTL,
      int cacheSize,
      boolean setExpected) {
    return newFixture(
        databaseType,
        consumers,
        cacheTTL,
        cacheSize,
        SLA_USER_AGENT,
        SLA_USER_TTL,
        false,
        setExpected);
  }

  private Fixture newFixture(
      DatabaseType databaseType,
      List<CQLSessionCache.DeactivatedTenantConsumer> consumers,
      Duration cacheTTL,
      int cacheSize,
      String slaUserAgent,
      Duration slaUserTTL,
      boolean useFakeTicker,
      boolean setExpected) {

    var credentialsFactory = mock(CqlCredentialsFactory.class);
    var sessionFactory = mock(CQLSessionCache.SessionFactory.class);

    CqlCredentials expectedCredentials = null;
    CqlSession expectedSession = null;
    if (setExpected) {
      expectedCredentials = mock(CqlCredentials.class);
      when(credentialsFactory.apply(AUTH_TOKEN)).thenReturn(expectedCredentials);

      expectedSession = mock(CqlSession.class);
      when(sessionFactory.apply(TENANT_ID, expectedCredentials)).thenReturn(expectedSession);
    }

    // spy because the cache will create metrics when created
    var meterRegistry = spy(new SimpleMeterRegistry());

    var fakeTicker = useFakeTicker ? new FakeTicker() : null;

    // run async on caller thread so they will reliably complete before the test ends, used for
    // removal callback
    var cache =
        new CQLSessionCache(
            databaseType,
            cacheTTL,
            cacheSize,
            slaUserAgent, // Updated to use new parameter
            slaUserTTL, // Updated to use new parameter
            credentialsFactory,
            sessionFactory,
            meterRegistry,
            consumers,
            true,
            fakeTicker);

    return new Fixture(
        cache,
        expectedCredentials,
        credentialsFactory,
        expectedSession,
        sessionFactory,
        meterRegistry,
        fakeTicker);
  }
}
