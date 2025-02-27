package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObjectName;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assertions and helpers to run tests against a {@link BaseTaskTestTask}. */
public class BaseTaskAssertions<
    TaskT extends BaseTask<SchemaT, ResultSupplierT, ResultT>,
    SchemaT extends SchemaObject,
    ResultSupplierT extends BaseTask.UniSupplier<ResultT>,
    ResultT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskAssertions.class);

  public final TaskT task;
  private final CommandContext<SchemaT> commandContext;

  public BaseTaskAssertions(TaskT task, CommandContext<SchemaT> commandContext) {
    this.task = task;
    this.commandContext = commandContext;
  }

  /** Mock a table schema object with the given keyspace and table name. */
  public static TableSchemaObject mockTable(String keyspaceName, String tableName) {
    TableSchemaObject mockTable = mock(TableSchemaObject.class);

    when(mockTable.keyspaceName()).thenReturn(CqlIdentifier.fromInternal(keyspaceName));
    when(mockTable.tableName()).thenReturn(CqlIdentifier.fromInternal(tableName));
    when(mockTable.type()).thenReturn(SchemaObject.SchemaObjectType.TABLE);
    when(mockTable.name()).thenReturn(new SchemaObjectName(keyspaceName, tableName));
    return mockTable;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> assertCompleted() {

    LOGGER.warn("assertCompleted() starting \ntask={}", PrettyPrintable.pprint(task));
    task.execute(commandContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem(Duration.ofSeconds(1)) // wait up to 1 second, so retries can be handled
        .assertCompleted();
    LOGGER.warn("assertCompleted() finished \ntask={}", PrettyPrintable.pprint(task));
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> maybeAddFailure(
      Throwable expectedException) {
    task.maybeAddFailure(expectedException);
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> setStatus(
      Task.TaskStatus status) {
    task.setStatus(status);
    return this;
  }

  /** Throw exception every time {@link BaseTask#buildResultSupplier(CommandContext)} is called. */
  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> doThrowOnBuildResultSupplier(
      RuntimeException exception) {
    doThrow(exception).when(task).buildResultSupplier(any());
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT>
      doReturnErrorOnMaybeHandleException(RuntimeException expected, RuntimeException returned) {
    doReturn(returned)
        .when(task)
        .maybeHandleException(any(), expected == null ? any() : eq(expected));
    return this;
  }

  /** Throw exception FIRST time {@link BaseTask#buildResultSupplier(CommandContext)} is called. */
  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT>
      doThrowOnceOnBuildResultSupplier(Throwable expectedException) {
    AtomicBoolean firstCall = new AtomicBoolean(true);

    doAnswer(
            invocation -> {
              if (firstCall.getAndSet(false)) {
                throw expectedException; // Throw exception on the first call
              }
              return invocation
                  .callRealMethod(); // Or return null/appropriate value if using a mock
            })
        .when(task)
        .buildResultSupplier(any());
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> assertStatus(
      Task.TaskStatus status, String message) {
    assertThat(task.status()).as("Status should be %s when: %s", status, message).isEqualTo(status);
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> assertHasFailure(
      Class<? extends Throwable> clazz, String message) {
    assertThat(task.failure())
        .as("task should have the failure class %s when: %s", clazz.getSimpleName(), message)
        .isPresent()
        .get()
        .isInstanceOf(clazz);
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> assertHasFailure(
      Throwable throwable, String message) {
    assertThat(task.failure())
        .as("task should have the failure %s when: %s", throwable.toString(), message)
        .isPresent()
        .get()
        .isSameAs(throwable);
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> assertFailureEmpty(
      String message) {
    assertThat(task.failure()).as("task failure should be empty when: %s", message).isEmpty();
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT>
      verifyBuildResultSupplierCalled(int times, String message) {
    verify(
            task,
            times(times)
                .description("execute() called %s times when: %s".formatted(times, message)))
        .buildResultSupplier(any());
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT>
      verifyMaybeHandleExceptionOnce(RuntimeException exception, String msg) {
    verify(
            task,
            times(1)
                .description("maybeHandleException() called %s times when: %s".formatted(1, msg)))
        .maybeHandleException(any(), eq(exception));
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyOnCompletionCalled(
      int times, String message) {

    verify(
            task,
            times(times)
                .description("onCompletion() called %s times when: %s".formatted(times, message)))
        .onCompletion(any(), any());
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyOnCompletionResult(
      ResultT expectedResult, String message) {
    verify(
            task,
            times(1)
                .description(
                    "onCompletion() called 1 time with result %s when: %s"
                        .formatted(Objects.toString(expectedResult, "NULL"), message)))
        .onCompletion(eq(expectedResult), any());
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyOnCompletionThrowable(
      Throwable expectedThrowable, String message) {
    verify(
            task,
            times(1)
                .description(
                    "onCompletion() called 1 time with throwable %s when: %s"
                        .formatted(Objects.toString(expectedThrowable, "NULL"), message)))
        .onCompletion(any(), eq(expectedThrowable));
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyOnSuccessCalled(
      int times, String message) {
    verify(
            task,
            times(times)
                .description("onSuccess() called %s times when: %s".formatted(times, message)))
        .onSuccess(any());
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyOnSuccessResult(
      ResultT expected, String message) {
    verify(
            task,
            times(1)
                .description(
                    "onSuccess() called with assertions result %s when: %s"
                        .formatted(expected, message)))
        .onSuccess(expected);
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyOneWarning(
      WarningException.Code code, String message) {

    assertThat(task.allWarnings())
        .as("Warning exists with code=%s when %s:".formatted(code, message))
        .hasSize(1)
        .anyMatch(warning -> (warning != null) && warning.code.equals(code.name()));
    return this;
  }

  public BaseTaskAssertions<TaskT, SchemaT, ResultSupplierT, ResultT> verifyWarningContains(
      String contains, String message) {

    assertThat(task.allWarnings())
        .as("Warning message contains assertions when: %s".formatted(message))
        .hasSize(1)
        .first()
        .satisfies(
            warningException -> {
              assertThat(warningException.getMessage()).contains(contains);
            });
    return this;
  }
}
