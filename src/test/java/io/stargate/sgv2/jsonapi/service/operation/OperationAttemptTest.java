package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link OperationAttempt}. */
public class OperationAttemptTest {

  private static final TestData TEST_DATA = new TestData();

  @Test
  public void noExecuteStatementWhenError() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();
    var msg = "starting with ERROR";
    var expectedException = new RuntimeException(msg);

    fixture
        .maybeAddFailure(expectedException)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .assertFailure(expectedException, msg)
        .verifyExecuteStatementCalled(0, msg)
        .verifyOnCompletionCalled(0, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void errorWhenNotReadyState() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();

    var msg = "starting when UNINITIALIZED";
    fixture
        .setStatus(OperationAttempt.OperationStatus.UNINITIALIZED)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .assertFailure(IllegalStateException.class, msg)
        .verifyExecuteStatementCalled(0, msg)
        .verifyOnCompletionCalled(0, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void exceptionFromExecuteStatement() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();

    var msg = "Exception thrown by executeStatement()";
    var expectedException = new RuntimeException(msg);

    fixture
        .setStatus(OperationAttempt.OperationStatus.READY)
        .doThrowOnExecuteStatement(expectedException)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .assertFailure(expectedException, msg)
        .verifyExecuteStatementCalled(1, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResultSet(null, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void successFromExecuteStatement() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();
    var msg = "success from executeStatement()";

    fixture
        .setStatus(OperationAttempt.OperationStatus.READY)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.COMPLETED, msg)
        .assertFailureEmpty(msg)
        .verifyExecuteStatementCalled(1, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResultSet(fixture.resultSet(), msg)
        .verifyOnSuccessCalled(1, msg)
        .verifyOnSuccessResultSet(fixture.resultSet(), msg);
  }

  @Test
  public void retryAndSuccess() {

    var fixture = TEST_DATA.operationAttempt().fixtureWithOneRetry();
    var msg = "one retry after executeStatementFailure()";
    var exception = new RuntimeException(msg);

    fixture
        .setStatus(OperationAttempt.OperationStatus.READY)
        .doThrowOnceOnExecuteStatement(exception)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.COMPLETED, msg)
        .assertFailureEmpty(msg)
        .verifyExecuteStatementCalled(2, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResultSet(fixture.resultSet(), msg)
        .verifyOnSuccessCalled(1, msg)
        .verifyOnSuccessResultSet(fixture.resultSet(), msg);
  }

  @Test
  public void retryAndFail() {

    var fixture = TEST_DATA.operationAttempt().fixtureWithOneRetry();
    var msg = "retry and fail executeStatementFailure()";
    var exception = new RuntimeException(msg);

    fixture
        .setStatus(OperationAttempt.OperationStatus.READY)
        .doThrowOnExecuteStatement(exception)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .assertFailure(exception, msg)
        .verifyExecuteStatementCalled(2, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResultSet(null, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void handledErrorIsTracked() {

    var msg = "handledErrorIsTracked()";
    var originalException = new RuntimeException("handledErrorIsTracked() - original exception");
    var handledException = new RuntimeException("handledErrorIsTracked() - handled exception");

    var fixture = TEST_DATA.operationAttempt().fixtureWithHandledError(handledException);

    fixture
        .setStatus(OperationAttempt.OperationStatus.READY)
        .doThrowOnExecuteStatement(originalException)
        .assertCompleted()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .assertFailure(handledException, msg)
        .verifyExecuteStatementCalled(1, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResultSet(null, msg)
        .verifyOnSuccessCalled(0, msg);
  }
}
