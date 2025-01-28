package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.retry.RetryDecision;

/**
 * In Astra, CQL router will be used and retry on the next node will fail. So, the decision will be
 * to retry on the same node.
 */
public class AstraCqlRetryPolicy extends BaseCqlRetryPolicy {

  @Override
  protected RetryDecision retryDecisionForUnavailable() {
    return RetryDecision.RETRY_SAME;
  }

  @Override
  protected RetryDecision retryDecisionForRequestAborted() {
    return RetryDecision.RETRY_SAME;
  }

  @Override
  protected RetryDecision retryDecisionForErrorResponse() {
    return RetryDecision.RETRY_SAME;
  }
}
