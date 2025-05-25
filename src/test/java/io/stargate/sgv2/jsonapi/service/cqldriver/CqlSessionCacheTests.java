package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.benmanes.caffeine.cache.Ticker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

/** Tests for {@link CQLSessionCache}. */
public class CqlSessionCacheTests {

  // None of these TTLs are real, we are using hte Fake Ticker to control the time
  private static final String AUTH_TOKEN = "authToken";
  private static final Tenant TENANT = Tenant.create(DatabaseType.ASTRA, "test-tenant");
  private static final Duration CACHE_TTL = Duration.ofSeconds(10);
  private final int CACHE_MAX_SIZE = 10;

  private static final String SLA_USER_AGENT_STRING = "SLA_USER_AGENT";
  private static final UserAgent SLA_USER_AGENT = new UserAgent(SLA_USER_AGENT_STRING);
  private static final Duration SLA_USER_TTL = Duration.ofSeconds(5);


  @Test
  public void sessionFactoryCalledOnceOnly() {

    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // auth token was passed to credentials factory
    verify(fixture.credentialsFactory).apply(AUTH_TOKEN);

    // tenant and expected credentials passed to the session factory
    verify(fixture.sessionFactory).apply(TENANT, fixture.expectedCredentials);

    verifyNoMoreInteractions(fixture.credentialsFactory, fixture.sessionFactory);

    // Clear invocation counts, stubb remains
    clearInvocations(fixture.credentialsFactory, fixture.sessionFactory);

    // Second call to check cache hit
    var actualSession2 = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);
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
  public void sessionClosedAndListenerCalledOnForced() {
    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // cache to force eviction
    fixture.cache.clearCache();
    // Session must be closed when evicted
    verify(fixture.expectedSession).close();

    // should have called our listener when evicted
    verify(fixture.listener).accept(eq(TENANT));

    assertThat(fixture.cache.peekSession(TENANT, AUTH_TOKEN, null))
        .as("Session is not in cache after eviction")
        .isNotPresent();
  }

  @Test
  public void essionClosedAndListenerCalledOnExpired() {

    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);
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
    verify(fixture.listener).accept(TENANT);

    // session should not be in the cache
    assertThat(fixture.cache.peekSession(TENANT, AUTH_TOKEN, null))
        .as("Session is not in cache after expried")
        .isNotPresent();
  }

  @Test
  public void listenerNotCalledUntilEvicted() {

    var fixture = newFixture();

    var actualSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);

    // must not call consumer until evicted
    verifyNoInteractions(fixture.listener);

    // force eviction
    fixture.cache.clearCache();
    verify(fixture.listener).accept(TENANT);
    verifyNoMoreInteractions(fixture.listener);
  }

  @Test
  public void listenerErrorSwallowed() {

    var listener1 = listenerWithLogging();
    doThrow(new RuntimeException("test exception")).when(listener1).accept(any());
    var listener2 = listenerWithLogging();

    var fixture = newFixture(
        List.of(listener1, listener2), CACHE_TTL, CACHE_MAX_SIZE, SLA_USER_AGENT, SLA_USER_TTL);

    var actualSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // force eviction to get callbacks
    fixture.cache.clearCache();

    // should call both consumers, even though consumer 1 throws an exception
    verify(listener1).accept(TENANT);
    verify(listener2).accept(TENANT);
    verifyNoMoreInteractions(listener1, listener2);
  }

  @Test
  public void nullCredentialsCausesError() {

    var fixture = newFixture();
    when(fixture.credentialsFactory.apply(AUTH_TOKEN)).thenReturn(null);

    assertThrows(
        IllegalStateException.class,
        () -> {
          fixture.cache.getSession(TENANT, AUTH_TOKEN, null);
        });
  }


  /**
   * {@link io.stargate.sgv2.jsonapi.util.DynamicTTLCacheTests} tests that the dynamic TTL works,
   * this checks that because we can have multiple CqlSession instances in flights for the same
   * tenant + credentials we are closing and handling that.
   * <p>
   * There can be multiple CqlSession instances in flight because the cache is lazy, it may not
   * evict the items until the next time the cache is accessed.
   */
  @Test
  public void expireKeepsSessionAfterNonSLAUserRead() {

    var fixture = newFixture();

    // request a session using the SLA user agent, this will be expired after SLA_USER_TTL
    var expectedSlaSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(expectedSlaSession));
    var actualSlaSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, SLA_USER_AGENT);

    assertThat(actualSlaSession)
        .as("Session from cache is expectedSlaSession")
        .isSameAs(expectedSlaSession);

    // advance the time past the TTL for the SLA user, cache may cleanup in the background
    fixture.ticker.advance(SLA_USER_TTL.plusNanos(10));

    // Now get the session with non SLA user agent
    var expectedNonSlaSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(expectedNonSlaSession));
    var actualNonSlaSession = fixture.cache.getSession(TENANT, AUTH_TOKEN, null);

    assertThat(actualNonSlaSession)
        .as("Session from cache is expectedNonSlaSession, new session obtained")
        .isSameAs(expectedNonSlaSession);

    // advance the time past the TTL for the non SLA user, this will also go past the SLA user TTL
    fixture.ticker.advance(CACHE_TTL.plusNanos(10));

    // the user agent is not part of the key, only tenant and auth, user agent is used to
    // create the TTL for the key
    // for sanity, peeking session is not present with either
    assertThat(fixture.cache.peekSession(TENANT, AUTH_TOKEN, SLA_USER_AGENT))
        .as("SLA Session is not present in cache when peeking with SLA agent - after non SLA expiry")
        .isNotPresent();
    assertThat(fixture.cache.peekSession(TENANT, AUTH_TOKEN, null))
        .as("SLA Session is not present in cache when peeking non SLA agent - after non SLA expiry")
            .isNotPresent();

    // Now give the cache time to clean, which means it should actually evict items
    fixture.cache.cleanUp();

    // close() should be called on both sessions
    verify(expectedSlaSession).close();
    verify(expectedNonSlaSession).close();
  }

  // =======================================================
  // Helpers / no more tests below
  // =======================================================

  private Tenant thisTenant(int i) {
    return Tenant.create(TENANT.databaseType(), "%s-%s".formatted(TENANT.toString(), i));
  }

  private String thisAuthToken(int i) {
    return "%s-%s".formatted(AUTH_TOKEN, i);
  }

  private CQLSessionCache.DeactivatedTenantListener listenerWithLogging() {
    var consumer = mock(CQLSessionCache.DeactivatedTenantListener.class);
    doAnswer(
            invocation -> {
              var tenantId = invocation.getArgument(0);
              var cause = invocation.getArgument(1);
              System.out.println(
                  "Eviction listener called for tenantId: " + tenantId + ", cause: " + cause);
              return null;
            })
        .when(consumer)
        .accept(any());
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
      CQLSessionCache.DeactivatedTenantListener listener,
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
        List.of(),
        CACHE_TTL,
        CACHE_MAX_SIZE,
        SLA_USER_AGENT,
        SLA_USER_TTL);
  }


  private Fixture newFixture(
      List<CQLSessionCache.DeactivatedTenantListener> listeners,
      Duration cacheTTL,
      int cacheSize,
      UserAgent slaUserAgent,
      Duration slaUserTTL) {

    if (listeners == null) {
      listeners = List.of(listenerWithLogging());
    }

    var credentialsFactory = mock(CqlCredentialsFactory.class);
    var expectedCredentials = mock(CqlCredentials.class);
    when(credentialsFactory.apply(AUTH_TOKEN)).thenReturn(expectedCredentials);

    var sessionFactory = mock(CQLSessionCache.SessionFactory.class);
    var expectedSession = mock(CqlSession.class);
      when(sessionFactory.apply(TENANT, expectedCredentials))
          .thenReturn(CompletableFuture.completedFuture(expectedSession));

    var meterRegistry = new SimpleMeterRegistry();

    var fakeTicker = new FakeTicker();

    // run async on caller thread so they will reliably complete before the test ends, used for
    // removal callback
    var cache =
        new CQLSessionCache(
            cacheSize,
            cacheTTL,
            slaUserAgent, // Updated to use new parameter
            slaUserTTL, // Updated to use new parameter
            credentialsFactory,
            sessionFactory,
            meterRegistry,
            listeners,
            true,
            fakeTicker);

    return new Fixture(
        cache,
        listeners.getFirst(),
        expectedCredentials,
        credentialsFactory,
        expectedSession,
        sessionFactory,
        meterRegistry,
        fakeTicker);
  }
}
