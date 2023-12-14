package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retry policy that retries to the same node in the query plan. DefaultRetryPolicy retries to the
 * next node in the query plan in some of the error cases and this implementation overrides that
 * behavior.
 */
public class SameNodeRetryPolicy extends DefaultRetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryPolicy.class);
  private final String logPrefix;

  public SameNodeRetryPolicy(DriverContext context, String profileName) {
    super(context, profileName);
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry, to the same node in the query plan.
   *
   * <p>Otherwise, the exception is rethrown.
   */
  @Override
  @Deprecated
  public RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {

    RetryDecision decision = (retryCount == 0) ? RetryDecision.RETRY_SAME : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_UNAVAILABLE, logPrefix, cl, required, alive, retryCount);
    }

    return decision;
  }

  /**
   * This implementation retries on the same node if the connection was closed, and rethrows
   * (assuming a driver bug) in all other cases.
   */
  @Override
  @Deprecated
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {

    RetryDecision decision =
        (error instanceof ClosedConnectionException || error instanceof HeartbeatException)
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_ABORTED, logPrefix, retryCount, error);
    }

    return decision;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation rethrows read and write failures, and retries other errors on the same
   * node.
   */
  @Override
  @Deprecated
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {

    RetryDecision decision =
        (error instanceof ReadFailureException || error instanceof WriteFailureException)
            ? RetryDecision.RETHROW
            : RetryDecision.RETRY_SAME;

    if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_ERROR, logPrefix, retryCount, error);
    }

    return decision;
  }
}
