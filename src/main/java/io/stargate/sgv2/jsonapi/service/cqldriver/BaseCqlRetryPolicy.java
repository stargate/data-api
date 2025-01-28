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
 * compared to {@link DefaultRetryPolicy}. Logs are recorded at the INFO level instead of TRACE
 * level, aligning with DataAPI logging standards.
 */
public class BaseCqlRetryPolicy implements RetryPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(BaseCqlRetryPolicy.class);
  private static final int MAX_RETRIES = Integer.getInteger("stargate.cql_proxy.max_retries", 3);

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of {@code MAX_RETRIES} retry (to the same node), and
   * only if enough replicas had responded to the read request but data was not retrieved amongst
   * those. That usually means that enough replicas are alive to satisfy the consistency, but the
   * coordinator picked a dead one for data retrieval, not having detected that replica as dead yet.
   * The reasoning is that by the time we get the timeout, the dead replica will likely have been
   * detected as dead and the retry has a high chance of success. Otherwise, the exception is
   * rethrown.
   */
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

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of {@code MAX_RETRIES} retries (to the same node),
   * and only for {@code WriteType.CAS} and {@code WriteType.SIMPLE} write. The reasoning is that
   * collections use lightweight transactions, the write type is either CAS or SIMPLE and tables use
   * SIMPLE for writes.
   */
  @Override
  public RetryVerdict onWriteTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {

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

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of {@code MAX_RETRIES} retry, to the next node in the
   * query plan. The rationale is that the first coordinator might have been network-isolated from
   * all other nodes (thinking they're down), but still able to communicate with the client; in that
   * case, retrying on the same host has almost no chance of success, but moving to the next host
   * might solve the issue.
   *
   * <p>Note: In Astra, CQL router will be used and retry on the next node will fail. So, the
   * decision will be to retry on the same node.
   */
  @Override
  public RetryVerdict onUnavailableVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {

    RetryDecision retryDecision =
        (retryCount < MAX_RETRIES) ? retryDecisionForUnavailable() : RetryDecision.RETHROW;

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

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries on the next node if the connection was closed, and rethrows
   * (assuming a driver bug) in all other cases.
   *
   * <p>Note: In Astra, CQL router will be used and retry on the next node will fail. So, the
   * decision will be to retry on the same node.
   */
  @Override
  public RetryVerdict onRequestAbortedVerdict(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {

    RetryDecision retryDecision =
        (retryCount < MAX_RETRIES
                && (error instanceof ClosedConnectionException
                    || error instanceof HeartbeatException))
            ? retryDecisionForRequestAborted()
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

  /**
   * {@inheritDoc}
   *
   * <p>This implementation rethrows read and write failures, and retries other errors on the next
   * node.
   *
   * <p>Note: In Astra, CQL router will be used and retry on the next node will fail. So, the
   * decision will be to retry on the same node.
   */
  @Override
  public RetryVerdict onErrorResponseVerdict(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {

    // Issue1830: CASWriteUnknownException and TruncateException have been included in the default
    // case.
    var retryDecision =
        switch (error) {
          case ReadFailureException e -> RetryDecision.RETHROW;
          case WriteFailureException e -> RetryDecision.RETHROW;
          default ->
              (retryCount < MAX_RETRIES) ? retryDecisionForErrorResponse() : RetryDecision.RETHROW;
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
  public void close() {}

  protected RetryDecision retryDecisionForUnavailable() {
    return RetryDecision.RETRY_NEXT;
  }

  protected RetryDecision retryDecisionForRequestAborted() {
    return RetryDecision.RETRY_NEXT;
  }

  protected RetryDecision retryDecisionForErrorResponse() {
    return RetryDecision.RETRY_NEXT;
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
}
