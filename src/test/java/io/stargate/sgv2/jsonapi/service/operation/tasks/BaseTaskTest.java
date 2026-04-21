package io.stargate.sgv2.jsonapi.service.operation.tasks;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BaseTask}.
 *
 * <p><b>NOTE:</b> This is uses the {@link BaseTaskTestTask} which is neither a read, write, or
 * schema attempt it is a generic attempt that is used to test the operation attempt class
 */
public class BaseTaskTest {

  private static final BaseTaskTestData TEST_DATA = new BaseTaskTestData();

  @Test
  public void noExecuteStatementWhenError() {

    var msg = "starting with ERROR";
    var expectedException = new RuntimeException(msg);

    TEST_DATA
        .defaultTask()
        .maybeAddFailure(expectedException)
        .assertCompleted()
        .assertStatus(Task.TaskStatus.ERROR, msg)
        .assertHasFailure(expectedException, msg)
        .verifyBuildResultSupplierCalled(0, msg)
        .verifyOnCompletionCalled(0, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void errorWhenNotReadyState() {

    var msg = "starting when UNINITIALIZED";
    TEST_DATA
        .defaultTask()
        .setStatus(Task.TaskStatus.UNINITIALIZED)
        .assertCompleted()
        .assertStatus(Task.TaskStatus.ERROR, msg)
        .assertHasFailure(IllegalStateException.class, msg)
        .verifyBuildResultSupplierCalled(0, msg)
        .verifyOnCompletionCalled(0, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void exceptionFromExecuteStatement() {

    var msg = "Exception thrown by executeStatement()";
    var expectedException = new RuntimeException(msg);

    TEST_DATA
        .defaultTask()
        .setStatus(Task.TaskStatus.READY)
        .doThrowOnBuildResultSupplier(expectedException)
        .doReturnErrorOnMaybeHandleException(
            null, expectedException) // no mapping, always return the same
        .assertCompleted()
        .assertStatus(Task.TaskStatus.ERROR, msg)
        .assertHasFailure(expectedException, msg)
        .verifyBuildResultSupplierCalled(1, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResult(null, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void successFromExecuteStatement() {

    var msg = "success from executeStatement()";

    var task = TEST_DATA.defaultTask();
    task.setStatus(Task.TaskStatus.READY)
        .assertCompleted()
        .assertStatus(Task.TaskStatus.COMPLETED, msg)
        .assertFailureEmpty(msg)
        .verifyBuildResultSupplierCalled(1, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResult(task.task.TASK_RESULT, msg)
        .verifyOnSuccessCalled(1, msg)
        .verifyOnSuccessResult(task.task.TASK_RESULT, msg);
  }

  @Test
  public void retryAndSuccess() {

    var msg = "one retry after executeStatementFailure()";
    var exception = new RuntimeException(msg);

    var task = TEST_DATA.taskWithOneRetry();
    task.setStatus(Task.TaskStatus.READY)
        .doThrowOnceOnBuildResultSupplier(exception)
        .assertCompleted()
        .assertStatus(Task.TaskStatus.COMPLETED, msg)
        .assertFailureEmpty(msg)
        .verifyBuildResultSupplierCalled(2, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResult(task.task.TASK_RESULT, msg)
        .verifyOnSuccessCalled(1, msg)
        .verifyOnSuccessResult(task.task.TASK_RESULT, msg);
  }

  @Test
  public void retryAndFail() {

    var msg = "retry and fail executeStatementFailure()";
    var exception = new RuntimeException(msg);

    var task = TEST_DATA.taskWithOneRetry();
    task.setStatus(Task.TaskStatus.READY)
        .doThrowOnBuildResultSupplier(exception)
        .doReturnErrorOnMaybeHandleException(null, exception) // no mapping, always return the same
        .assertCompleted()
        .assertStatus(Task.TaskStatus.ERROR, msg)
        .assertHasFailure(exception, msg)
        .verifyBuildResultSupplierCalled(2, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResult(null, msg)
        .verifyOnSuccessCalled(0, msg);
  }

  @Test
  public void handledErrorIsTracked() {

    var msg = "handledErrorIsTracked() with no retry";
    var originalException = new RuntimeException("handledErrorIsTracked() - original exception");
    var handledException = new RuntimeException("handledErrorIsTracked() - handled exception");

    var task = TEST_DATA.taskWithOneRetry();
    task.setStatus(Task.TaskStatus.READY)
        .doThrowOnBuildResultSupplier(originalException)
        .doReturnErrorOnMaybeHandleException(originalException, handledException)
        .assertCompleted()
        .assertStatus(Task.TaskStatus.ERROR, msg)
        .assertHasFailure(handledException, msg)
        .verifyMaybeHandleExceptionOnce(originalException, msg)
        .verifyBuildResultSupplierCalled(2, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResult(null, msg)
        .verifyOnSuccessCalled(0, msg);
  }
}
