package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlRetryPolicy extends DefaultRetryPolicy {
  private final String logPrefix;
  private static final Logger LOG = LoggerFactory.getLogger(CqlRetryPolicy.class);
  private static final int MAX_RETRIES = Integer.getInteger("stargate.cql_proxy.max_retries", 3);

  public CqlRetryPolicy(DriverContext context, String profileName) {
    super(context, profileName);
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
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
      LOG.info(RETRYING_ON_WRITE_TIMEOUT, logPrefix, cl, writeType, blockFor, received, retryCount);
    }
    return () -> retryDecision;
  }
}
