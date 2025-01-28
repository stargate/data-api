package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom retry policy tailored for DataAPI, providing distinct retry logic for specific scenarios
 * compared to {@link DefaultRetryPolicy}.
 *
 * <p>Key differences from the default implementation:
 *
 * <ul>
 *   <li>Overrides {@code onWriteTimeoutVerdict} and {@code onErrorResponseVerdict} to customize
 *       retry behavior for write timeouts and error responses.
 *   <li>Other methods retain the default logic but include additional log messages.
 *   <li>Logs provide enhanced details, including errors and retry decisions, for improved debugging
 *       and monitoring.
 *   <li>Logs are recorded at the INFO level instead of TRACE level, aligning with DataAPI logging
 *       standards.
 * </ul>
 */
public class CqlRetryPolicy implements RetryPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(CqlRetryPolicy.class);
  private static final int MAX_RETRIES = Integer.getInteger("stargate.cql_proxy.max_retries", 3);

  @Override
  public RetryVerdict onReadTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {

    RetryDecision retryDecision =
        (retryCount < MAX_RETRIES && received >= blockFor && !dataPresent)
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Retrying on read timeout on same host (consistency: {}, required responses: {}, received responses: {}, data retrieved: {}, retries: {}, retry decision: {})",
          cl,
          blockFor,
          received,
          dataPresent,
          retryCount,
          retryDecision);
    }

    return () -> retryDecision;
  }

  @Override
  public RetryVerdict onWriteTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {

    // Collections use lightweight transactions, the write type is either CAS or SIMPLE. Tables use
    // SIMPLE for writes.
    final RetryDecision retryDecision =
        (retryCount < MAX_RETRIES && (writeType == WriteType.CAS || writeType == WriteType.SIMPLE))
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Retrying on write timeout on same host (consistency: {}, write type: {}, required acknowledgments: {}, received acknowledgments: {}, retries: {}, retry decision: {})",
          cl,
          writeType,
          blockFor,
          received,
          retryCount,
          retryDecision);
    }

    return () -> retryDecision;
  }

  @Override
  public RetryVerdict onUnavailableVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {

    RetryDecision retryDecision =
        (retryCount < MAX_RETRIES) ? RetryDecision.RETRY_NEXT : RetryDecision.RETHROW;

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Retrying on unavailable exception on next host (consistency: {}, required replica: {}, alive replica: {}, retries: {}, retry decision: {})",
          cl,
          required,
          alive,
          retryCount,
          retryDecision);
    }

    return () -> retryDecision;
  }

  @Override
  public RetryVerdict onRequestAbortedVerdict(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {

    RetryDecision retryDecision =
        (error instanceof ClosedConnectionException || error instanceof HeartbeatException)
            ? RetryDecision.RETRY_NEXT
            : RetryDecision.RETHROW;

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Retrying on aborted request on next host (retries: {}, error: {}, retry decision: {})",
          retryCount,
          error,
          retryDecision);
    }

    return () -> retryDecision;
  }

  @Override
  public RetryVerdict onErrorResponseVerdict(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {

    var retryDecision =
        switch (error) {
          case CASWriteUnknownException e -> handleErrorResponseRetry(retryCount);
          case TruncateException e -> handleErrorResponseRetry(retryCount);
          case ReadFailureException e -> handleErrorResponseRetry(retryCount);
          case WriteFailureException e -> handleErrorResponseRetry(retryCount);
          default -> RetryDecision.RETHROW;
        };

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Retrying on node error on next host (retries: {}, error: {}, retry decision: {})",
          retryCount,
          error,
          retryDecision);
    }

    return () -> retryDecision;
  }

  @Override
  @Deprecated
  public RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    throw new UnsupportedOperationException("onReadTimeout");
  }

  @Override
  @Deprecated
  public RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    throw new UnsupportedOperationException("onWriteTimeout");
  }

  @Override
  @Deprecated
  public RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    throw new UnsupportedOperationException("onUnavailable");
  }

  @Override
  @Deprecated
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    throw new UnsupportedOperationException("onRequestAborted");
  }

  @Override
  @Deprecated
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    throw new UnsupportedOperationException("onErrorResponse");
  }

  @Override
  public void close() {}

  private RetryDecision handleErrorResponseRetry(int retryCount) {
    return (retryCount < MAX_RETRIES) ? RetryDecision.RETRY_NEXT : RetryDecision.RETHROW;
  }
}
