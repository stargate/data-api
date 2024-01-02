package io.stargate.sgv2.jsonapi.service.cqldriver;

import static com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy.RETRYING_ON_UNAVAILABLE;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This retry policy while executing cql statements is useful when the requests are delegated to a
 * proxy layer which is responsible for retrying the requests. This policy will only retry once if
 * the intermediate layer is unavailable for some reason. In all other cases, this will rethrow the
 * exception to avoid retrying the requests at the driver level.
 */
public class CqlProxyRetryPolicy implements RetryPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(CqlProxyRetryPolicy.class);
  private final String logPrefix;

  public CqlProxyRetryPolicy(DriverContext context, String profileName) {
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
  }

  @Override
  public RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryVerdict onReadTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    RetryDecision retryDecision =
        onReadTimeout(request, cl, blockFor, received, dataPresent, retryCount);
    return () -> retryDecision;
  }

  @Override
  public RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryVerdict onWriteTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    RetryDecision retryDecision =
        onWriteTimeout(request, cl, writeType, blockFor, received, retryCount);
    return () -> retryDecision;
  }

  @Override
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

  @Override
  public RetryVerdict onUnavailableVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    RetryDecision retryDecision = onUnavailable(request, cl, required, alive, retryCount);
    return () -> retryDecision;
  }

  @Override
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryVerdict onRequestAbortedVerdict(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    RetryDecision retryDecision = onRequestAborted(request, error, retryCount);
    return () -> retryDecision;
  }

  @Override
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    return RetryDecision.RETHROW;
  }

  @Override
  public RetryVerdict onErrorResponseVerdict(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    RetryDecision retryDecision = onErrorResponse(request, error, retryCount);
    return () -> retryDecision;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
