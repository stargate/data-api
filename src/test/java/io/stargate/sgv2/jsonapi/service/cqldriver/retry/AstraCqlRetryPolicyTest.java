package io.stargate.sgv2.jsonapi.service.cqldriver.retry;

import static com.datastax.oss.driver.api.core.DefaultConsistencyLevel.QUORUM;
import static com.datastax.oss.driver.api.core.retry.RetryDecision.*;

import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.servererrors.CASWriteUnknownException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import io.stargate.sgv2.jsonapi.service.cqldriver.AstraCqlRetryPolicy;
import org.junit.Test;

public class AstraCqlRetryPolicyTest extends RetryPolicyTestBase {

  public AstraCqlRetryPolicyTest() {
    super(new AstraCqlRetryPolicy(null, null));
  }

  @Test
  public void shouldProcessUnavailable() {
    assertOnUnavailable(QUORUM, 2, 1, 0).hasDecision(RETRY_SAME);
    assertOnUnavailable(QUORUM, 2, 1, 1).hasDecision(RETRY_SAME);
    assertOnUnavailable(QUORUM, 2, 1, 2).hasDecision(RETRY_SAME);
    assertOnUnavailable(QUORUM, 2, 1, 3).hasDecision(RETHROW);
  }

  @Test
  public void shouldProcessAbortedRequest() {
    assertOnRequestAborted(ClosedConnectionException.class, 0).hasDecision(RETRY_SAME);
    assertOnRequestAborted(ClosedConnectionException.class, 1).hasDecision(RETRY_SAME);
    assertOnRequestAborted(ClosedConnectionException.class, 2).hasDecision(RETRY_SAME);
    assertOnRequestAborted(ClosedConnectionException.class, 3).hasDecision(RETHROW);

    assertOnRequestAborted(HeartbeatException.class, 0).hasDecision(RETRY_SAME);
    assertOnRequestAborted(HeartbeatException.class, 1).hasDecision(RETRY_SAME);
    assertOnRequestAborted(HeartbeatException.class, 2).hasDecision(RETRY_SAME);
    assertOnRequestAborted(HeartbeatException.class, 3).hasDecision(RETHROW);

    assertOnRequestAborted(Throwable.class, 0).hasDecision(RETHROW);
  }

  @Test
  public void shouldProcessErrorResponse() {
    // rethrow on ReadFailureException and WriteFailureException
    assertOnErrorResponse(ReadFailureException.class, 0).hasDecision(RETHROW);
    assertOnErrorResponse(WriteFailureException.class, 0).hasDecision(RETHROW);

    // Issue1830 - retry 3 times on CASWriteUnknownException and TruncateException
    assertOnErrorResponse(CASWriteUnknownException.class, 0).hasDecision(RETRY_SAME);
    assertOnErrorResponse(CASWriteUnknownException.class, 1).hasDecision(RETRY_SAME);
    assertOnErrorResponse(CASWriteUnknownException.class, 2).hasDecision(RETRY_SAME);
    assertOnErrorResponse(CASWriteUnknownException.class, 3).hasDecision(RETHROW);

    assertOnErrorResponse(TruncateException.class, 0).hasDecision(RETRY_SAME);
    assertOnErrorResponse(TruncateException.class, 1).hasDecision(RETRY_SAME);
    assertOnErrorResponse(TruncateException.class, 2).hasDecision(RETRY_SAME);
    assertOnErrorResponse(TruncateException.class, 3).hasDecision(RETHROW);
  }
}
