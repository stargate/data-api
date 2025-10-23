package io.stargate.sgv2.jsonapi.service.cqldriver.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import org.assertj.core.api.AbstractAssert;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class RetryPolicyTestBase {
  private final RetryPolicy policy;

  @Mock private Request request;

  protected RetryPolicyTestBase(RetryPolicy policy) {
    this.policy = policy;
  }

  protected RetryVerdictAssert assertOnReadTimeout(
      ConsistencyLevel cl, int blockFor, int received, boolean dataPresent, int retryCount) {
    return new RetryVerdictAssert(
        policy.onReadTimeoutVerdict(request, cl, blockFor, received, dataPresent, retryCount));
  }

  protected RetryVerdictAssert assertOnWriteTimeout(
      ConsistencyLevel cl, WriteType writeType, int blockFor, int received, int retryCount) {
    return new RetryVerdictAssert(
        policy.onWriteTimeoutVerdict(request, cl, writeType, blockFor, received, retryCount));
  }

  protected RetryVerdictAssert assertOnUnavailable(
      ConsistencyLevel cl, int required, int alive, int retryCount) {
    return new RetryVerdictAssert(
        policy.onUnavailableVerdict(request, cl, required, alive, retryCount));
  }

  protected RetryVerdictAssert assertOnRequestAborted(
      Class<? extends Throwable> errorClass, int retryCount) {
    return new RetryVerdictAssert(
        policy.onRequestAbortedVerdict(request, mock(errorClass), retryCount));
  }

  protected RetryVerdictAssert assertOnErrorResponse(
      Class<? extends CoordinatorException> errorClass, int retryCount) {
    return new RetryVerdictAssert(
        policy.onErrorResponseVerdict(request, mock(errorClass), retryCount));
  }

  public static class RetryVerdictAssert extends AbstractAssert<RetryVerdictAssert, RetryVerdict> {
    RetryVerdictAssert(RetryVerdict actual) {
      super(actual, RetryVerdictAssert.class);
    }

    public RetryVerdictAssert hasDecision(RetryDecision decision) {
      assertThat(actual.getRetryDecision()).isEqualTo(decision);
      return this;
    }
  }
}
