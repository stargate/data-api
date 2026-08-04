package io.stargate.sgv2.jsonapi.api.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class DatabaseReadinessCheckTest {

  private static final Duration TIMEOUT = Duration.ofMillis(100);
  private static final String ASTRA_TENANT_ID = "60b5dccb-e91d-4a60-987b-7588cd8aa1e3";

  private CQLSessionCache sessionCache;
  private CqlSession session;
  private RequestContext requestContext;
  private DatabaseReadinessCheck readinessCheck;

  @BeforeEach
  public void setup() {
    sessionCache = mock(CQLSessionCache.class);
    session = mock(CqlSession.class);
    requestContext =
        new RequestContext(
            Tenant.create(DatabaseType.ASTRA, ASTRA_TENANT_ID, "us-west-2"),
            "astra-token",
            new UserAgent("Datastax-SLA-Checker"));
    readinessCheck = new DatabaseReadinessCheck(sessionCache, TIMEOUT);
  }

  @Test
  public void successfulCheckUsesRequestContextAndAsyncDistributedQuery() {
    var resultSet = mock(AsyncResultSet.class);
    var resultFuture = new CompletableFuture<AsyncResultSet>();
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().item(session));
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(resultFuture);

    var subscriber =
        readinessCheck
            .check(requestContext)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    subscriber.assertSubscribed().assertNotTerminated();
    resultFuture.complete(resultSet);
    subscriber.awaitItem().assertItem(null).assertCompleted();

    verify(sessionCache).getSession(same(requestContext));
    var statementCaptor = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(session).executeAsync(statementCaptor.capture());
    assertThat(statementCaptor.getValue().getQuery())
        .isEqualTo("SELECT * FROM datastax_sla.check LIMIT 1");
    assertThat(statementCaptor.getValue().getTimeout()).isEqualTo(TIMEOUT);
    assertThat(statementCaptor.getValue().getConsistencyLevel())
        .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
    assertThat(statementCaptor.getValue().getExecutionProfileName()).isEqualTo("table-read");
    verify(session, never()).execute(any(SimpleStatement.class));
    verify(session, never()).isClosed();
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }

  @Test
  public void sessionAcquisitionFailureIsPropagated() {
    when(sessionCache.getSession(requestContext))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("Cannot connect")));

    readinessCheck
        .check(requestContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(IllegalStateException.class, "Cannot connect");

    verify(session, never()).executeAsync(any(SimpleStatement.class));
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }

  @Test
  public void asynchronousQueryFailureIsPropagated() {
    var resultFuture = new CompletableFuture<AsyncResultSet>();
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().item(session));
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(resultFuture);

    var subscriber =
        readinessCheck
            .check(requestContext)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());
    resultFuture.completeExceptionally(new IllegalStateException("Query failed"));

    subscriber.awaitFailure().assertFailedWith(IllegalStateException.class, "Query failed");
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }

  @Test
  public void reactiveTimeoutBoundsSessionAcquisition() {
    readinessCheck = new DatabaseReadinessCheck(sessionCache, Duration.ofMillis(20));
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().nothing());

    readinessCheck
        .check(requestContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(TimeoutException.class);

    verify(session, never()).executeAsync(any(SimpleStatement.class));
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }

  @Test
  public void reactiveTimeoutBoundsAsynchronousQuery() {
    readinessCheck = new DatabaseReadinessCheck(sessionCache, Duration.ofMillis(20));
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().item(session));
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(new CompletableFuture<>());

    readinessCheck
        .check(requestContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(TimeoutException.class);

    verify(session).executeAsync(any(SimpleStatement.class));
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }
}
