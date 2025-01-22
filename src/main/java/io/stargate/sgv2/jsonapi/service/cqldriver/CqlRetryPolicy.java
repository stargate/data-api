package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlRetryPolicy extends DefaultRetryPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(CqlRetryPolicy.class);
  private static final int MAX_RETRIES = Integer.getInteger("stargate.cql_proxy.max_retries", 3);

  @VisibleForTesting
  public static final String RETRYING_ON_READ_TIMEOUT =
      "[{}] Retrying on read timeout on same host (consistency: {}, required responses: {}, "
          + "received responses: {}, data retrieved: {}, retries: {}, retry decision: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_WRITE_TIMEOUT =
      "[{}] Retrying on write timeout on same host (consistency: {}, write type: {}, "
          + "required acknowledgments: {}, received acknowledgments: {}, retries: {}, retry decision: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_UNAVAILABLE =
      "[{}] Retrying on unavailable exception on next host (consistency: {}, "
          + "required replica: {}, alive replica: {}, retries: {}, retry decision: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_ABORTED =
      "[{}] Retrying on aborted request on next host (retries: {}, error: {}, retry decision: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_ERROR =
      "[{}] Retrying on node error on next host (retries: {}, error: {}, retry decision: {})";

  private final String logPrefix;

  public CqlRetryPolicy(DriverContext context, String profileName) {
    super(context, profileName);
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
  }

  @Override
  public RetryVerdict onReadTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    var retryVerdict =
        super.onReadTimeoutVerdict(request, cl, blockFor, received, dataPresent, retryCount);
    var retryDecision = retryVerdict.getRetryDecision();

    if (LOG.isInfoEnabled()) {
      LOG.info(
          RETRYING_ON_READ_TIMEOUT,
          logPrefix,
          cl,
          blockFor,
          received,
          false,
          retryCount,
          retryDecision);
    }

    return retryVerdict;
  }

  @Override
  public RetryVerdict onWriteTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    final RetryDecision retryDecision;
    if (retryCount < MAX_RETRIES && (writeType == WriteType.CAS || writeType == WriteType.SIMPLE)) {
      retryDecision = RetryDecision.RETRY_SAME;
    } else {
      retryDecision = RetryDecision.RETHROW;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(
          RETRYING_ON_WRITE_TIMEOUT,
          logPrefix,
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
    var retryVerdict = super.onUnavailableVerdict(request, cl, required, alive, retryCount);
    var retryDecision = retryVerdict.getRetryDecision();

    if (LOG.isInfoEnabled()) {
      LOG.info(RETRYING_ON_UNAVAILABLE, logPrefix, cl, required, alive, retryCount, retryDecision);
    }

    return retryVerdict;
  }

  @Override
  public RetryVerdict onRequestAbortedVerdict(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    var retryVerdict = super.onRequestAbortedVerdict(request, error, retryCount);
    var retryDecision = retryVerdict.getRetryDecision();

    if (LOG.isInfoEnabled()) {
      LOG.info(RETRYING_ON_ABORTED, logPrefix, retryCount, error, retryDecision);
    }

    return retryVerdict;
  }

  @Override
  public RetryVerdict onErrorResponseVerdict(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    var retryVerdict = super.onErrorResponseVerdict(request, error, retryCount);
    var retryDecision = retryVerdict.getRetryDecision();

    if (LOG.isInfoEnabled()) {
      LOG.info(RETRYING_ON_ERROR, logPrefix, retryCount, error, retryDecision);
    }

    return retryVerdict;
  }
}
