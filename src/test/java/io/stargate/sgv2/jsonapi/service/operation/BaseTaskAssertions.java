package io.stargate.sgv2.jsonapi.service.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTaskAssertions<FixtureT> {

  private static Logger LOGGER = LoggerFactory.getLogger(BaseTaskAssertions.class);

  private final FixtureT fixture;
  public final TestBaseTask task;
  private final CommandContext<SchemaT> commandContext;
  private final DefaultDriverExceptionHandler.Factory<?> exceptionHandlerFactory;

  public BaseTaskAssertions(
      FixtureT fixture,
      TestBaseTask task,
      CommandContext<SchemaT> commandContext,
      DefaultDriverExceptionHandler.Factory<?> exceptionHandlerFactory) {
    this.fixture = fixture;
    this.task = task;
    this.commandContext = commandContext;
    this.exceptionHandlerFactory = exceptionHandlerFactory;
  }

  public FixtureT assertCompleted() {

    LOGGER.warn("assertCompleted() starting \ntask={}", task.toString(true));
    task
        .execute(commandContext)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem(Duration.ofSeconds(1)) // wait up to 1 second, so retries can be handled
        .assertCompleted();
    LOGGER.warn("assertCompleted() finished \ntask={}", task.toString(true));
    return fixture;
  }

  public FixtureT maybeAddFailure(Throwable expectedException) {
    task.maybeAddFailure(expectedException);
    return fixture;
  }

  public FixtureT setStatus(Task.TaskStatus status) {
    task.setStatus(status);
    return fixture;
  }

  public FixtureT doThrowOnBuildStatementContext(Throwable expectedException) {
    doThrow(expectedException).when(task).buildStatementContext(any());
    return fixture;
  }

  public FixtureT doThrowOnceOnExecuteStatement(Throwable expectedException) {
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
        .buildStatementContext(any());
    return fixture;
  }

  public FixtureT assertStatus(OperationAttempt.OperationStatus status, String message) {
    assertThat(task.status())
        .as("Status should be %s when: %s", status, message)
        .isEqualTo(status);
    return fixture;
  }

  public FixtureT assertFailure(Class<? extends Throwable> clazz, String message) {
    assertThat(task.failure())
        .as("task should have the failure class %s when: %s", clazz.getSimpleName(), message)
        .isPresent()
        .get()
        .isInstanceOf(clazz);
    return fixture;
  }

  public FixtureT assertFailure(Throwable throwable, String message) {
    assertThat(task.failure())
        .as("task should have the failure %s when: %s", throwable.toString(), message)
        .isPresent()
        .get()
        .isSameAs(throwable);
    return fixture;
  }

  public FixtureT assertFailureEmpty(String message) {
    assertThat(task.failure()).as("task failure should be empty when: %s", message).isEmpty();
    return fixture;
  }

  public FixtureT verifyExecuteStatementCalled(int times, String message) {
    verify(
        task,
            times(times)
                .description("execute() called %s times when: %s".formatted(times, message)))
        .buildStatementContext(any());
    return fixture;
  }

  public FixtureT verifyOnCompletionCalled(int times, String message) {

    verify(
        task,
            times(times)
                .description("onCompletion() called %s times when: %s".formatted(times, message)))
        .onCompletion(any(), any(), any());
    return fixture;
  }

  public FixtureT verifyOnCompletionResultSet(AsyncResultSet expectedResultSet, String message) {
    verify(
        task,
            times(1)
                .description(
                    "onCompletion() called 1 time with resultset %s when: %s"
                        .formatted(Objects.toString(expectedResultSet, "NULL"), message)))
        .onCompletion(any(), eq(expectedResultSet), any());
    return fixture;
  }

  public FixtureT verifyOnCompletionThrowable(Throwable expectedThrowable, String message) {
    verify(
        task,
            times(1)
                .description(
                    "onCompletion() called 1 time with throwable %s when: %s"
                        .formatted(Objects.toString(expectedThrowable, "NULL"), message)))
        .onCompletion(any(), any(), eq(expectedThrowable));
    return fixture;
  }

  public FixtureT verifyOnSuccessCalled(int times, String message) {
    verify(
        task,
            times(times)
                .description("onSuccess() called %s times when: %s".formatted(times, message)))
        .onSuccess(any());
    return fixture;
  }

  public FixtureT verifyOnSuccessResultSet(AsyncResultSet expected, String message) {
    verify(
        task,
            times(1)
                .description(
                    "onSuccess() called with assertions resultset %s when: %s"
                        .formatted(expected, message)))
        .onSuccess(expected);
    return fixture;
  }

  public FixtureT verifyOneWarning(WarningException.Code code, String message) {

    assertThat(task.allWarnings())
        .as("Warning exists with code=%s when %s:".formatted(code, message))
        .hasSize(1)
        .anyMatch(
            warning -> (warning instanceof WarningException) && warning.code.equals(code.name()));
    return fixture;
  }

  public FixtureT verifyWarningContains(String contains, String message) {

    assertThat(task.allWarnings())
        .as("Warning message contains assertions when: %s".formatted(message))
        .hasSize(1)
        .first()
        .satisfies(
            warningException -> {
              assertThat(warningException.getMessage()).contains(contains);
            });
    return fixture;
  }
}
