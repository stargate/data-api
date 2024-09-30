package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttempt;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class OperationAttemptTestData extends TestDataSuplier {

  public OperationAttemptTestData(TestData testData) {
    super(testData);
  }

  public OperationAttemptFixture emptyFixture() {

    return newFixture(null, null, null);
  }

  public OperationAttemptFixture fixtureWithOneRetry() {

    var retryPolicy =
        new OperationAttempt.RetryPolicy(1, Duration.ofMillis(1)) {
          @Override
          public boolean shouldRetry(Throwable throwable) {
            return true;
          }
        };

    return newFixture(null, retryPolicy, null);
  }

  public OperationAttemptFixture fixtureWithHandledError(RuntimeException handledException) {

    var exceptionHandler = spy(new TableDriverExceptionHandler());
    doReturn(handledException).when(exceptionHandler).maybeHandle(any(), any());

    return newFixture(null, null, exceptionHandler);
  }

  private OperationAttemptFixture newFixture(
      AsyncResultSet resultSet,
      OperationAttempt.RetryPolicy retryPolicy,
      TableDriverExceptionHandler exceptionHandler) {

    if (resultSet == null) {
      resultSet = testData.resultSet().emptyResultSet();
    }
    if (retryPolicy == null) {
      retryPolicy = OperationAttempt.RetryPolicy.NO_RETRY;
    }
    if (exceptionHandler == null) {
      exceptionHandler = spy(new TableDriverExceptionHandler());
    }

    // spy() the attempt and handler so we get default behaviour and can track calls to the methods
    var attempt =
        spy(
            new OperationAttemptFixture.TestOperationAttempt(
                0, testData.schemaObject().emptyTableSchemaObject(), retryPolicy, resultSet));

    return new OperationAttemptFixture(
        attempt, mock(CommandQueryExecutor.class), exceptionHandler, resultSet);
  }

  public record OperationAttemptFixture(
      TestOperationAttempt attempt,
      CommandQueryExecutor queryExecutor,
      DriverExceptionHandler<TableSchemaObject> exceptionHandler,
      AsyncResultSet resultSet) {

    public OperationAttemptFixture maybeAddFailure(Throwable expectedException) {
      attempt.maybeAddFailure(expectedException);
      return this;
    }

    public OperationAttemptFixture setStatus(OperationAttempt.OperationStatus status) {
      attempt.setStatus(status);
      return this;
    }

    public OperationAttemptFixture doThrowOnExecuteStatement(Throwable expectedException) {
      doThrow(expectedException).when(attempt()).executeStatement(queryExecutor());
      return this;
    }

    public OperationAttemptFixture doThrowOnceOnExecuteStatement(Throwable expectedException) {
      AtomicBoolean firstCall = new AtomicBoolean(true);

      doAnswer(
              invocation -> {
                if (firstCall.getAndSet(false)) {
                  throw expectedException; // Throw exception on the first call
                }
                return invocation
                    .callRealMethod(); // Or return null/appropriate value if using a mock
              })
          .when(attempt())
          .executeStatement(queryExecutor());
      return this;
    }

    public OperationAttemptFixture assertCompleted() {
      attempt()
          .execute(queryExecutor(), exceptionHandler())
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem(Duration.ofSeconds(1)) // wit up to 1 second, so retries can be handled
          .assertCompleted();
      return this;
    }

    public OperationAttemptFixture assertStatus(
        OperationAttempt.OperationStatus status, String message) {
      assertThat(attempt().status())
          .as("Status should be %s when: %s", status, message)
          .isEqualTo(status);
      return this;
    }

    public OperationAttemptFixture assertFailure(Class<? extends Throwable> clazz, String message) {
      assertThat(attempt().failure())
          .as("Attempt should have the failure class %s when: %s", clazz.getSimpleName(), message)
          .isPresent()
          .get()
          .isInstanceOf(clazz);
      return this;
    }

    public OperationAttemptFixture assertFailure(Throwable throwable, String message) {
      assertThat(attempt().failure())
          .as("Attempt should have the failure %s when: %s", throwable.toString(), message)
          .isPresent()
          .get()
          .isSameAs(throwable);
      return this;
    }

    public OperationAttemptFixture assertFailureEmpty(String message) {
      assertThat(attempt().failure())
          .as("Attempt failure should be empty when: %s", message)
          .isEmpty();
      return this;
    }

    public OperationAttemptFixture verifyExecuteStatementCalled(int times, String message) {
      verify(
              attempt(),
              times(times)
                  .description("execute() called %s times when: %s".formatted(times, message)))
          .executeStatement(queryExecutor());
      return this;
    }

    public OperationAttemptFixture verifyOnCompletionCalled(int times, String message) {

      verify(
              attempt(),
              times(times)
                  .description("onCompletion() called %s times when: %s".formatted(times, message)))
          .onCompletion(eq(exceptionHandler()), any(), any());
      return this;
    }

    public OperationAttemptFixture verifyOnCompletionResultSet(
        AsyncResultSet expectedResultSet, String message) {
      verify(
              attempt(),
              times(1)
                  .description(
                      "onCompletion() called 1 time with resultset %s when: %s"
                          .formatted(Objects.toString(expectedResultSet, "NULL"), message)))
          .onCompletion(eq(exceptionHandler()), eq(expectedResultSet), any());
      return this;
    }

    public OperationAttemptFixture verifyOnCompletionThrowable(
        Throwable expectedThrowable, String message) {
      verify(
              attempt(),
              times(1)
                  .description(
                      "onCompletion() called 1 time with throwable %s when: %s"
                          .formatted(Objects.toString(expectedThrowable, "NULL"), message)))
          .onCompletion(eq(exceptionHandler()), any(), eq(expectedThrowable));
      return this;
    }

    public OperationAttemptFixture verifyOnSuccessCalled(int times, String message) {
      verify(
              attempt(),
              times(times)
                  .description("onSuccess() called %s times when: %s".formatted(times, message)))
          .onSuccess(any());
      return this;
    }

    public OperationAttemptFixture verifyOnSuccessResultSet(
        AsyncResultSet expected, String message) {
      verify(
              attempt(),
              times(1)
                  .description(
                      "onSuccess() called with expected resultset %s when: %s"
                          .formatted(resultSet, message)))
          .onSuccess(expected);
      return this;
    }

    public static class TestOperationAttempt
        extends OperationAttempt<TestOperationAttempt, TableSchemaObject> {

      private final AsyncResultSet resultSet;

      TestOperationAttempt(
          int position,
          TableSchemaObject schemaObject,
          RetryPolicy retryPolicy,
          AsyncResultSet resultSet) {
        super(position, schemaObject, retryPolicy);
        this.resultSet = resultSet;
      }

      // these overrides here make the functions accessible to the test data class without changing
      // the visibility

      @Override
      protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {
        return Uni.createFrom().item(this.resultSet);
      }

      @Override
      protected TestOperationAttempt onCompletion(
          DriverExceptionHandler<TableSchemaObject> exceptionHandler,
          AsyncResultSet resultSet,
          Throwable throwable) {
        return super.onCompletion(exceptionHandler, resultSet, throwable);
      }

      @Override
      protected TestOperationAttempt onSuccess(AsyncResultSet resultSet) {
        return super.onSuccess(resultSet);
      }

      @Override
      protected TestOperationAttempt setStatus(OperationStatus newStatus) {
        return super.setStatus(newStatus);
      }
    }
  }
}
