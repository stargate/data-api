package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OperationAttempt}.
 *
 * <p><b>NOTE:</b> This is uses the {@link TestOperationAttempt} which is neither a read, write, or
 * schema attempt it is a generic attempt that is used to test the operation attempt class - it does
 * not call to the QueryExecutor
 */
public class OperationAttemptTest {

  private static final TestData TEST_DATA = new TestData();

  @Test
  public void noExecuteStatementWhenError() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();
    var msg = "starting with ERROR";
    var expectedException = new RuntimeException(msg);

    fixture
        .attempt()
        .maybeAddFailure(expectedException)
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .attempt()
        .assertFailure(expectedException, msg)
        .attempt()
        .verifyExecuteStatementCalled(0, msg)
        .attempt()
        .verifyOnCompletionCalled(0, msg)
        .attempt()
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void errorWhenNotReadyState() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();

    var msg = "starting when UNINITIALIZED";
    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.UNINITIALIZED)
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .attempt()
        .assertFailure(IllegalStateException.class, msg)
        .attempt()
        .verifyExecuteStatementCalled(0, msg)
        .attempt()
        .verifyOnCompletionCalled(0, msg)
        .attempt()
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void exceptionFromExecuteStatement() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();

    var msg = "Exception thrown by executeStatement()";
    var expectedException = new RuntimeException(msg);

    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.READY)
        .attempt()
        .doThrowOnBuildStatementContext(expectedException)
        .exceptionHandler()
        .doMaybeHandleException(expectedException) // no mapping, always return the same
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .attempt()
        .assertFailure(expectedException, msg)
        .attempt()
        .verifyExecuteStatementCalled(1, msg)
        .attempt()
        .verifyOnCompletionCalled(1, msg)
        .attempt()
        .verifyOnCompletionResultSet(null, msg)
        .attempt()
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void successFromExecuteStatement() {

    var fixture = TEST_DATA.operationAttempt().emptyFixture();
    var msg = "success from executeStatement()";

    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.READY)
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.COMPLETED, msg)
        .attempt()
        .assertFailureEmpty(msg)
        .attempt()
        .verifyExecuteStatementCalled(1, msg)
        .attempt()
        .verifyOnCompletionCalled(1, msg)
        .attempt()
        .verifyOnCompletionResultSet(fixture.resultSet(), msg)
        .attempt()
        .verifyOnSuccessCalled(1, msg)
        .attempt()
        .verifyOnSuccessResultSet(fixture.resultSet(), msg);
  }

  @Test
  public void retryAndSuccess() {

    var fixture = TEST_DATA.operationAttempt().fixtureWithOneRetry();
    var msg = "one retry after executeStatementFailure()";
    var exception = new RuntimeException(msg);

    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.READY)
        .attempt()
        .doThrowOnceOnExecuteStatement(exception)
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.COMPLETED, msg)
        .attempt()
        .assertFailureEmpty(msg)
        .attempt()
        .verifyExecuteStatementCalled(2, msg)
        .attempt()
        .verifyOnCompletionCalled(1, msg)
        .attempt()
        .verifyOnCompletionResultSet(fixture.resultSet(), msg)
        .attempt()
        .verifyOnSuccessCalled(1, msg)
        .attempt()
        .verifyOnSuccessResultSet(fixture.resultSet(), msg);
  }

  @Test
  public void retryAndFail() {

    var fixture = TEST_DATA.operationAttempt().fixtureWithOneRetry();
    var msg = "retry and fail executeStatementFailure()";
    var exception = new RuntimeException(msg);

    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.READY)
        .attempt()
        .doThrowOnBuildStatementContext(exception)
        .exceptionHandler()
        .doMaybeHandleException(exception) // no mapping, always return the same
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .attempt()
        .assertFailure(exception, msg)
        .attempt()
        .verifyExecuteStatementCalled(2, msg)
        .attempt()
        .verifyOnCompletionCalled(1, msg)
        .attempt()
        .verifyOnCompletionResultSet(null, msg)
        .attempt()
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void handledErrorIsTracked() {

    var msg = "handledErrorIsTracked() with no retry";
    var originalException = new RuntimeException("handledErrorIsTracked() - original exception");
    var handledException = new RuntimeException("handledErrorIsTracked() - handled exception");

    var fixture = TEST_DATA.operationAttempt().emptyFixture();

    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.READY)
        .attempt()
        .doThrowOnBuildStatementContext(originalException)
        .exceptionHandler()
        .doMaybeHandleException(originalException, handledException)
        .attempt()
        .assertCompleted()
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.ERROR, msg)
        .attempt()
        .assertFailure(handledException, msg)
        .exceptionHandler()
        .verifyMaybeHandleOnce(originalException, msg)
        .attempt()
        .verifyExecuteStatementCalled(1, msg)
        .attempt()
        .verifyOnCompletionCalled(1, msg)
        .attempt()
        .verifyOnCompletionResultSet(null, msg)
        .attempt()
        .verifyOnSuccessCalled(0, msg);
  }
}
