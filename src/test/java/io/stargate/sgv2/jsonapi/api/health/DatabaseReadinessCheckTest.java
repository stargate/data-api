package io.stargate.sgv2.jsonapi.api.health;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  private void stubSessionMetadata(NodeState... nodeStates) {
    var metadata = mock(Metadata.class);
    var nodes = new HashMap<UUID, Node>();
    for (NodeState nodeState : nodeStates) {
      var node = mock(Node.class);
      when(node.getState()).thenReturn(nodeState);
      nodes.put(UUID.randomUUID(), node);
    }
    when(metadata.getNodes()).thenReturn(Map.copyOf(nodes));
    when(session.getMetadata()).thenReturn(metadata);
  }

  @Test
  public void upNodeInSessionMetadataCompletesWithoutQuerying() {
    stubSessionMetadata(NodeState.DOWN, NodeState.UP);
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().item(session));

    readinessCheck
        .check(requestContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem()
        .assertItem(null)
        .assertCompleted();

    verify(sessionCache).getSession(same(requestContext));
    verify(session, never()).executeAsync(any(SimpleStatement.class));
    verify(session, never()).execute(any(SimpleStatement.class));
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }

  @Test
  public void noUpNodeInSessionMetadataFails() {
    stubSessionMetadata(NodeState.DOWN, NodeState.UNKNOWN);
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().item(session));

    readinessCheck
        .check(requestContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(IllegalStateException.class, "no node in the UP state");

    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }

  @Test
  public void emptySessionMetadataFails() {
    stubSessionMetadata();
    when(sessionCache.getSession(requestContext)).thenReturn(Uni.createFrom().item(session));

    readinessCheck
        .check(requestContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .assertFailedWith(IllegalStateException.class, "no node in the UP state");

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

    verify(session, never()).getMetadata();
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

    verify(session, never()).getMetadata();
    verify(sessionCache, never()).evictSession(any(RequestContext.class));
  }
}
