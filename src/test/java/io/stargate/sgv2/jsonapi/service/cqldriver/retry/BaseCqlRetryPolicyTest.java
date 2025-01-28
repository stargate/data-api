package io.stargate.sgv2.jsonapi.service.cqldriver.retry;

import static com.datastax.oss.driver.api.core.DefaultConsistencyLevel.QUORUM;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETHROW;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETRY_NEXT;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.RETRY_SAME;
import static com.datastax.oss.driver.api.core.servererrors.DefaultWriteType.CAS;
import static com.datastax.oss.driver.api.core.servererrors.DefaultWriteType.SIMPLE;

import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.BaseCqlRetryPolicy;
import org.junit.Test;

public class BaseCqlRetryPolicyTest extends RetryPolicyTestBase {

  public BaseCqlRetryPolicyTest() {
    super(new BaseCqlRetryPolicy(null, null));
  }

  @Test
  public void shouldProcessReadTimeouts() {
    assertOnReadTimeout(QUORUM, 2, 2, false, 0).hasDecision(RETRY_SAME);
    assertOnReadTimeout(QUORUM, 2, 2, false, 1).hasDecision(RETRY_SAME);
    assertOnReadTimeout(QUORUM, 2, 2, false, 2).hasDecision(RETRY_SAME);
    assertOnReadTimeout(QUORUM, 2, 2, false, 3).hasDecision(RETHROW);

    assertOnReadTimeout(QUORUM, 2, 2, true, 0).hasDecision(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 1, true, 0).hasDecision(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 1, false, 0).hasDecision(RETHROW);
  }

  @Test
  public void shouldProcessWriteTimeouts() {
    assertOnWriteTimeout(QUORUM, CAS, 2, 0, 0).hasDecision(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, CAS, 2, 0, 1).hasDecision(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, CAS, 2, 0, 2).hasDecision(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, CAS, 2, 0, 3).hasDecision(RETHROW);

    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 0).hasDecision(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 1).hasDecision(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 2).hasDecision(RETRY_SAME);
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 3).hasDecision(RETHROW);
  }

  @Test
  public void shouldProcessUnavailable() {
    assertOnUnavailable(QUORUM, 2, 1, 0).hasDecision(RETRY_NEXT);
    assertOnUnavailable(QUORUM, 2, 1, 1).hasDecision(RETRY_NEXT);
    assertOnUnavailable(QUORUM, 2, 1, 2).hasDecision(RETRY_NEXT);
    assertOnUnavailable(QUORUM, 2, 1, 3).hasDecision(RETHROW);
  }

  @Test
  public void shouldProcessAbortedRequest() {
    assertOnRequestAborted(ClosedConnectionException.class, 0).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(ClosedConnectionException.class, 1).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(ClosedConnectionException.class, 2).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(ClosedConnectionException.class, 3).hasDecision(RETHROW);

    assertOnRequestAborted(HeartbeatException.class, 0).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 1).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 2).hasDecision(RETRY_NEXT);
    assertOnRequestAborted(HeartbeatException.class, 3).hasDecision(RETHROW);

    assertOnRequestAborted(Throwable.class, 0).hasDecision(RETHROW);
  }

  @Test
  public void shouldProcessErrorResponse() {
    // rethrow on ReadFailureException and WriteFailureException
    assertOnErrorResponse(ReadFailureException.class, 0).hasDecision(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 0).hasDecision(RETHROW);

    // Issue1830 - retry 3 times on CASWriteUnknownException and TruncateException
    assertOnErrorResponse(CASWriteUnknownException.class, 0).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(CASWriteUnknownException.class, 1).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(CASWriteUnknownException.class, 2).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(CASWriteUnknownException.class, 3).hasDecision(RETHROW);

    assertOnErrorResponse(TruncateException.class, 0).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 1).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 2).hasDecision(RETRY_NEXT);
    assertOnErrorResponse(TruncateException.class, 3).hasDecision(RETHROW);
  }
}
