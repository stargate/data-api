package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.util.CacheTestsBase;
import io.stargate.sgv2.jsonapi.util.DynamicTTLCacheTests;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CQLSessionCache}.
 *
 * <p>The basics of the cache are tested in {@link DynamicTTLCacheTests}
 */
public class CqlSessionCacheTests extends CacheTestsBase {

  @Test
  public void sessionFactoryCalledOnceOnly() {

    var fixture = newFixture();

    var actualSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // auth token was passed to credentials factory
    verify(fixture.credentialsFactory).apply(TEST_CONSTANTS.AUTH_TOKEN);

    // tenant and expected credentials passed to the session factory
    verify(fixture.sessionFactory).apply(TEST_CONSTANTS.TENANT, fixture.expectedCredentials);

    verifyNoMoreInteractions(fixture.credentialsFactory, fixture.sessionFactory);

    // Clear invocation counts, stubb remains
    clearInvocations(fixture.credentialsFactory, fixture.sessionFactory);

    // Second call to check cache hit
    var actualSession2 =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();
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

    var actualSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // cache to force eviction
    fixture.cache.clearCache();
    // Session must be closed when evicted
    verify(fixture.expectedSession).close();

    // should have called our listener when evicted
    verify(fixture.listener).accept(eq(TEST_CONSTANTS.TENANT));

    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Session is not in cache after eviction")
        .isNotPresent();
  }

  @Test
  public void sessionClosedAndListenerCalledOnExpired() {

    var fixture = newFixture();

    var actualSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();
    ;
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // move the fake time forward to expire the session
    fixture.ticker.advance(LONG_TTL.plusSeconds(10));

    // let the cache cleanup
    fixture.cache.cleanUp();

    // Session must be closed when expired
    verify(fixture.expectedSession).close();

    // should have called our consumer when expired
    verify(fixture.listener).accept(TEST_CONSTANTS.TENANT);

    // session should not be in the cache
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Session is not in cache after expried")
        .isNotPresent();
  }

  @Test
  public void listenerNotCalledUntilEvicted() {

    var fixture = newFixture();

    var actualSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();
    ;

    // must not call consumer until evicted
    verifyNoInteractions(fixture.listener);

    // force eviction
    fixture.cache.clearCache();
    verify(fixture.listener).accept(TEST_CONSTANTS.TENANT);
    verifyNoMoreInteractions(fixture.listener);
  }

  @Test
  public void listenerErrorSwallowed() {

    var listener1 = listenerWithLogging();
    doThrow(new RuntimeException("test exception")).when(listener1).accept(any());
    var listener2 = listenerWithLogging();

    var fixture =
        newFixture(
            List.of(listener1, listener2),
            LONG_TTL,
            CACHE_MAX_SIZE,
            TEST_CONSTANTS.SLA_USER_AGENT,
            SHORT_TTL);

    var actualSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();
    ;
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    // force eviction to get callbacks
    fixture.cache.clearCache();

    // should call both consumers, even though consumer 1 throws an exception
    verify(listener1).accept(TEST_CONSTANTS.TENANT);
    verify(listener2).accept(TEST_CONSTANTS.TENANT);
    verifyNoMoreInteractions(listener1, listener2);
  }

  @Test
  public void nullCredentialsCausesError() {

    var fixture = newFixture();
    when(fixture.credentialsFactory.apply(TEST_CONSTANTS.AUTH_TOKEN)).thenReturn(null);

    assertThrows(
        IllegalStateException.class,
        () -> {
          fixture
              .cache
              .getSession(
                  TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
              .await()
              .indefinitely();
        });
  }

  /**
   * {@link io.stargate.sgv2.jsonapi.util.DynamicTTLCacheTests} tests that the dynamic TTL works,
   * this checks that because we can have multiple CqlSession instances in flights for the same
   * tenant + credentials we are closing and handling that.
   *
   * <p>There can be multiple CqlSession instances in flight because the cache is lazy, it may not
   * evict the items until the next time the cache is accessed.
   */
  @Test
  public void expireKeepsSessionAfterNonSLAUserRead() {

    var fixture = newFixture();

    // request a session using the SLA user agent, this will be expired after SLA_USER_TTL
    var expectedSlaSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(expectedSlaSession));
    var actualSlaSession =
        fixture
            .cache
            .getSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.SLA_USER_AGENT)
            .await()
            .indefinitely();

    assertThat(actualSlaSession)
        .as("Session from cache is expectedSlaSession")
        .isSameAs(expectedSlaSession);

    // advance the time past the TTL for the SLA user, cache may cleanup in the background
    fixture.ticker.advance(SHORT_TTL.plusNanos(10));

    // Now get the session with non SLA user agent
    var expectedNonSlaSession = mock(CqlSession.class);
    when(fixture.sessionFactory.apply(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(expectedNonSlaSession));
    var actualNonSlaSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();

    assertThat(actualNonSlaSession)
        .as("Session from cache is expectedNonSlaSession, new session obtained")
        .isSameAs(expectedNonSlaSession);

    // advance the time past the TTL for the non SLA user, this will also go past the SLA user TTL
    fixture.ticker.advance(LONG_TTL.plusNanos(10));

    // the user agent is not part of the key, only tenant and auth, user agent is used to
    // create the TTL for the key
    // for sanity, peeking session is not present with either
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.SLA_USER_AGENT))
        .as(
            "SLA Session is not present in cache when peeking with SLA agent - after non SLA expiry")
        .isNotPresent();
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("SLA Session is not present in cache when peeking non SLA agent - after non SLA expiry")
        .isNotPresent();

    // Now give the cache time to clean, which means it should actually evict items
    fixture.cache.cleanUp();

    // close() should be called on both sessions
    verify(expectedSlaSession).close();
    verify(expectedNonSlaSession).close();
  }

  @Test
  public void evictSessionWithTenantId() {
    var listener = listenerWithLogging();
    var fixture =
        newFixture(
            List.of(listener), LONG_TTL, CACHE_MAX_SIZE, TEST_CONSTANTS.SLA_USER_AGENT, SHORT_TTL);

    // Add a session to the cache and verify it is present
    var actualSession =
        fixture
            .cache
            .getSession(TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT)
            .await()
            .indefinitely();

    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);

    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Session is present in cache after adding")
        .isPresent();

    // Evict the session
    boolean evicted =
        fixture.cache.evictSession(
            TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT);

    // Verify eviction was successful
    assertThat(evicted).as("Eviction should return true when session exists").isTrue();

    // Verify session is closed
    verify(fixture.expectedSession).close();

    // verify consumer is called
    verify(listener).accept(TEST_CONSTANTS.TENANT);
    verifyNoMoreInteractions(listener);

    // Verify session is no longer in the cache
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Session is removed from cache after explicit eviction")
        .isNotPresent();
  }

  @Test
  public void evictSessionWithRequestContext() {
    var listener = listenerWithLogging();
    var fixture =
        newFixture(
            List.of(listener), LONG_TTL, CACHE_MAX_SIZE, TEST_CONSTANTS.SLA_USER_AGENT, SHORT_TTL);
    var requestContext =
        new RequestContext(
            Optional.of(TEST_CONSTANTS.TENANT),
            Optional.of(TEST_CONSTANTS.AUTH_TOKEN),
            null,
            TEST_CONSTANTS.USER_AGENT);

    // Add a session to the cache and verify it is present
    var actualSession = fixture.cache.getSession(requestContext).await().indefinitely();
    ;
    assertThat(actualSession)
        .as("Session from cache is instance from factory")
        .isSameAs(fixture.expectedSession);
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Session is present in cache after adding")
        .isPresent();

    // Evict the session using RequestContext
    boolean evicted = fixture.cache.evictSession(requestContext);

    // Verify eviction was successful
    assertThat(evicted).as("Eviction should return true when session exists").isTrue();

    // Verify session is closed
    verify(fixture.expectedSession).close();

    // verify consumer is called
    verify(listener).accept(TEST_CONSTANTS.TENANT);
    verifyNoMoreInteractions(listener);

    // Verify session is no longer in the cache
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Session is removed from cache after explicit eviction")
        .isNotPresent();
  }

  @Test
  public void evictSessionNotInCache() {
    var listener = listenerWithLogging();
    var fixture =
        newFixture(
            List.of(listener), LONG_TTL, CACHE_MAX_SIZE, TEST_CONSTANTS.SLA_USER_AGENT, SHORT_TTL);

    // Verify cache is empty
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Cache is empty before eviction")
        .isNotPresent();

    // Evict a non-existent session - should not throw
    boolean evicted =
        fixture.cache.evictSession(
            TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT);

    // Verify eviction returned false
    assertThat(evicted).as("Eviction returns false when no entry is removed").isFalse();

    // Verify cache is still empty
    assertThat(
            fixture.cache.peekSession(
                TEST_CONSTANTS.TENANT, TEST_CONSTANTS.AUTH_TOKEN, TEST_CONSTANTS.USER_AGENT))
        .as("Cache is still empty after eviction")
        .isNotPresent();

    // Verify no interactions with session factory
    verifyNoInteractions(fixture.sessionFactory);
  }

  // =======================================================
  // Helpers / no more tests below
  // =======================================================

  private String thisTenant(int i) {
    return "%s-%s".formatted(TEST_CONSTANTS.TENANT, i);
  }

  private String thisAuthToken(int i) {
    return "%s-%s".formatted(TEST_CONSTANTS.AUTH_TOKEN, i);
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

  private record Fixture(
      CQLSessionCache cache,
      CQLSessionCache.DeactivatedTenantListener listener,
      CqlCredentials expectedCredentials,
      CqlCredentialsFactory credentialsFactory,
      CqlSession expectedSession,
      CQLSessionCache.SessionFactory sessionFactory,
      MeterRegistry meterRegistry,
      DynamicTTLCacheTests.FakeTicker ticker) {

    CqlCredentials setTokenCredentials(String authToken) {
      var expectedCredentials = new CqlCredentials.TokenCredentials(authToken);
      when(this.credentialsFactory.apply(authToken)).thenReturn(expectedCredentials);
      return expectedCredentials;
    }
  }

  private Fixture newFixture() {
    return newFixture(null, LONG_TTL, CACHE_MAX_SIZE, TEST_CONSTANTS.SLA_USER_AGENT, SHORT_TTL);
  }

  private Fixture newFixture(
      List<CQLSessionCache.DeactivatedTenantListener> listeners,
      Duration cacheTTL,
      int cacheSize,
      String slaUserAgent,
      Duration slaUserTTL) {

    if (listeners == null) {
      listeners = List.of(listenerWithLogging());
    }

    var credentialsFactory = mock(CqlCredentialsFactory.class);
    var expectedCredentials = mock(CqlCredentials.class);
    when(credentialsFactory.apply(TEST_CONSTANTS.AUTH_TOKEN)).thenReturn(expectedCredentials);

    var sessionFactory = mock(CQLSessionCache.SessionFactory.class);
    var expectedSession = mock(CqlSession.class);
    when(sessionFactory.apply(TEST_CONSTANTS.TENANT, expectedCredentials))
        .thenReturn(CompletableFuture.completedFuture(expectedSession));

    var meterRegistry = new SimpleMeterRegistry();

    var fakeTicker = new DynamicTTLCacheTests.FakeTicker();

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
