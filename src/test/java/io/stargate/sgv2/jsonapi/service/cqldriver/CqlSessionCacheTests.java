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

  @Test
  public void cacheStartsEmtpy() {

    var fixture = newFixture();

    assertThat(fixture.cache.cacheSize()).as("Cache initialised with 0 sessions").isEqualTo(0);
  }

  @Test
  public void sessionFactoryCalledOnceOnly() {

    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(tenantId, authToken);
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
    var actualSession2 = fixture.cache.getSession(tenantId, authToken);
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
    var fixture = newFixture(DatabaseType.CASSANDRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken);
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
            DatabaseType.ASTRA, List.of(consumer), Duration.ofSeconds(20), CACHE_MAX_SIZE, false);

    for (int i = 0; i < CACHE_MAX_SIZE + 1; i++) {
      var thisTenantId = "%s-%s".formatted(tenantId, i);

      var expectedCredentials = mock(CqlCredentials.class);
      when(fixture.credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);

      var expectedSession = mock(CqlSession.class);
      when(fixture.sessionFactory.apply(thisTenantId, expectedCredentials))
          .thenReturn(expectedSession);

      var actualSession = fixture.cache.getSession(thisTenantId, authToken);
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
        newFixture(DatabaseType.ASTRA, List.of(consumer), CACHE_TTL, CACHE_MAX_SIZE, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken);
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
        newFixture(DatabaseType.ASTRA, List.of(consumer), CACHE_TTL, CACHE_MAX_SIZE, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken);
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
    var fixture = newFixture(DatabaseType.ASTRA, List.of(consumer), longTTL, CACHE_MAX_SIZE, true);

    var actualSession = fixture.cache.getSession(tenantId, authToken);

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
            DatabaseType.ASTRA, List.of(consumer1, consumer2), CACHE_TTL, CACHE_MAX_SIZE, true);
    var actualSession = fixture.cache.getSession(tenantId, authToken);
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
          fixture.cache.getSession(tenantId, authToken);
        });
  }

  @Test
  public void nullTenantIdToEmptyString() {
    var fixture = newFixture(DatabaseType.ASTRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, false);

    var expectedCredentials = mock(CqlCredentials.class);
    when(fixture.credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);

    var expectedSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply("", expectedCredentials)).thenReturn(expectedSession);

    var actualSession = fixture.cache.getSession(null, authToken);

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

    var fixture = newFixture();
    var actualSession = fixture.cache.getSession(tenantId, authToken);

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
    return newFixture(DatabaseType.ASTRA, List.of(), CACHE_TTL, CACHE_MAX_SIZE, true);
  }

  private Fixture newFixture(
      DatabaseType databaseType,
      List<CQLSessionCache.DeactivatedTenantConsumer> consumers,
      Duration cacheTTL,
      int cacheSize,
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
