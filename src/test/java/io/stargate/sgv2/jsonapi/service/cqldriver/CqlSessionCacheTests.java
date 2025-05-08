package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link CQLSessionCache}. */
public class CqlSessionCacheTests {

  private final String authToken = "authToken";
  private final String tenantId = "tenantId";
  private final Duration CACHE_TTL = Duration.ofSeconds(1);
  private final int CACHE_MAX_SIZE = 10;

  private final String SLA_USER_AGENT = "SLA_USER_AGENT";
  private final Duration SLA_USER_TTL = Duration.ofMillis(500);

  @Test
  public void cacheStartsEmtpy() {

    var fixture = newFixture();

    assertThat(fixture.cache.cacheSize()).as("Cache initialised with 0 sessions").isEqualTo(0);
  }

  @Test
  public void sessionFactoryCalledOnceOnly() {

    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(tenantId, authToken, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // auth token was passed to credentials factory
    verify(fixture.credentialsFactory).apply(authToken);

    // tenant and expected credentials passed to the session factory
    verify(fixture.sessionFactory).apply(tenantId, fixture.expectedCredentials);

    verifyNoMoreInteractions(fixture.credentialsFactory, fixture.sessionFactory);

    // Clear invocation counts, stubb remains
    clearInvocations(fixture.credentialsFactory, fixture.sessionFactory);

    // Second call to check cache hit
    var actualSession2 = fixture.cache.getSession(tenantId, authToken, null);
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
    var fixture = newFixture(DatabaseType.CASSANDRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // auth token was passed to credentials factory
    verify(fixture.credentialsFactory).apply(authToken);

    // tenant and expected credentials passed to the session factory
    verify(fixture.sessionFactory).apply(tenantId, fixture.expectedCredentials);

    verifyNoMoreInteractions(fixture.credentialsFactory, fixture.sessionFactory);
  }

  @Test
  public void limitedToMaxSize() {

    var consumer = consumerWithLogging();
    // use  very long TTL so removal cause is not expired
    var fixture =
        newFixture(
            DatabaseType.ASTRA, List.of(consumer), Duration.ofSeconds(20), CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, false);

    for (int i = 0; i < CACHE_MAX_SIZE + 1; i++) {
      var thisTenantId = "%s-%s".formatted(tenantId, i);

      var expectedCredentials = mock(CqlCredentials.class);
      when(fixture.credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);

      var expectedSession = mock(CqlSession.class);
      when(fixture.sessionFactory.apply(thisTenantId, expectedCredentials))
          .thenReturn(expectedSession);

      var actualSession = fixture.cache.getSession(thisTenantId, authToken, null);
      assertThat(actualSession)
          .as("Session from cache is instance from factory")
          .isSameAs(expectedSession);
    }

    fixture.cache.cleanUp();

    assertThat(fixture.cache.cacheSize()).as("Cache size is max size").isEqualTo(CACHE_MAX_SIZE);

    // consumer called with tenantId-0 because the size of the cache was exceeded
    verify(consumer).accept("%s-%s".formatted(tenantId, 0), RemovalCause.SIZE);
    verifyNoMoreInteractions(consumer);
  }

  @Test
  public void sessionClosedAndEvictionListenerCalledOnForced() {
    var consumer = consumerWithLogging();
    var fixture =
        newFixture(DatabaseType.ASTRA, List.of(consumer), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken, null);
    assertThat(fixture.cache.cacheSize()).as("Cache size is 1 after getting item").isEqualTo(1);

    ///  we dont care why the tenant was marked inactive, so do not need to wait for TTL, clear the
    // cache to force eviction
    fixture.cache.clearCache();
    // Session must be closed when evicted
    verify(fixture.expectedSession).close();

    // should have called our consumer when evicted
    verify(consumer).accept(eq(tenantId), any());

    assertThat(fixture.cache.cacheSize()).as("Cache size is 0 after eviction").isEqualTo(0);
  }

  @Test
  public void sessionClosedAndEvictionListenerCalledOnExpired() throws InterruptedException {

    var ttl = Duration.ofSeconds(1);
    var consumer = consumerWithLogging();
    var fixture =
        newFixture(DatabaseType.ASTRA, List.of(consumer), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // wait for the session to expire
    Thread.sleep(ttl.toMillis() * 2);

    // let the cache cleanup
    fixture.cache.cleanUp();

    // Session must be closed when expired
    verify(fixture.expectedSession).close();

    // should have called our consumer when expired
    verify(consumer).accept(tenantId, RemovalCause.EXPIRED);

    assertThat(fixture.cache.cacheSize()).as("Cache size is 0 after expiration").isEqualTo(0);
  }

  @Test
  public void evictionListenerNotCalledUntilEvicted() {

    var longTTL = Duration.ofSeconds(5);
    var consumer = mock(CQLSessionCache.DeactivatedTenantConsumer.class);
    var fixture = newFixture(DatabaseType.ASTRA, List.of(consumer), longTTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken, null);

    // must not call consumer until evicted
    verifyNoInteractions(consumer);

    // force eviction so not waiting for longTTL
    fixture.cache.clearCache();
    verify(consumer).accept(tenantId, RemovalCause.EXPLICIT);
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
            DatabaseType.ASTRA, List.of(consumer1, consumer2), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, true);
    var actualSession = fixture.cache.getSession(tenantId, authToken, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // force eviction to get callbacks
    fixture.cache.clearCache();
    // should call both consumers, even though consumer 1 throws an exception
    verify(consumer1).accept(tenantId, RemovalCause.EXPLICIT);
    verify(consumer2).accept(tenantId, RemovalCause.EXPLICIT);
    verifyNoMoreInteractions(consumer1, consumer2);
  }

  @Test
  public void nullCredentialsCausesError() {
    var fixture = newFixture();
    when(fixture.credentialsFactory.apply(authToken)).thenReturn(null);

    assertThrows(
        IllegalStateException.class,
        () -> {
          fixture.cache.getSession(tenantId, authToken, null);
        });
  }

  @Test
  public void nullTenantIdToEmptyString() {
    var fixture = newFixture(DatabaseType.ASTRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, false);

    var expectedCredentials = mock(CqlCredentials.class);
    when(fixture.credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);

    var expectedSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply("", expectedCredentials)).thenReturn(expectedSession);

    var actualSession = fixture.cache.getSession(null, authToken, null);

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

    var fixture = newFixture(DatabaseType.ASTRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL, true);
    var actualSession = fixture.cache.getSession(tenantId, authToken, null);

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
  public void expireAfterCreateUsesLoadingKey(){
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


  // =======================================================
  // Helpers / no more tests below
  // =======================================================
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

  private record Fixture(
      CQLSessionCache cache,
      CqlCredentials expectedCredentials,
      CqlCredentialsFactory credentialsFactory,
      CqlSession expectedSession,
      CQLSessionCache.SessionFactory sessionFactory,
      MeterRegistry meterRegistry) {}

  private Fixture newFixture() {
    return newFixture(DatabaseType.ASTRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, null, null, true);
  }

  private Fixture newFixture(
      DatabaseType databaseType,
      List<CQLSessionCache.DeactivatedTenantConsumer> consumers,
      Duration cacheTTL,
      int cacheSize,
      boolean setExpected) {
    return newFixture(databaseType, consumers, cacheTTL, cacheSize, null, null, setExpected);
  }

  private Fixture newFixture(
      DatabaseType databaseType,
      List<CQLSessionCache.DeactivatedTenantConsumer> consumers,
      Duration cacheTTL,
      int cacheSize,
      String slaUserAgent, // New parameter
      Duration slaUserTTL, // New parameter
      boolean setExpected) {

    var credentialsFactory = mock(CqlCredentialsFactory.class);
    var sessionFactory = mock(CQLSessionCache.SessionFactory.class);

    CqlCredentials expectedCredentials = null;
    CqlSession expectedSession = null;
    if (setExpected) {
      expectedCredentials = mock(CqlCredentials.class);
      when(credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);

      expectedSession = mock(CqlSession.class);
      when(sessionFactory.apply(tenantId, expectedCredentials)).thenReturn(expectedSession);
    }

    // spy because the cache will create metrics when created
    var meterRegistry = spy(new SimpleMeterRegistry());

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
            true);

    return new Fixture(
        cache,
        expectedCredentials,
        credentialsFactory,
        expectedSession,
        sessionFactory,
        meterRegistry);
  }
}
