package io.stargate.sgv2.jsonapi.service.operation;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataSuplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import java.time.Duration;

public class OperationAttemptTestData extends TestDataSuplier {

  public OperationAttemptTestData(TestData testData) {
    super(testData);
  }

  public OperationAttemptFixture<TestOperationAttempt, TableSchemaObject> emptyFixture() {

    return newFixture(null, null, null);
  }

  public OperationAttemptFixture<TestOperationAttempt, TableSchemaObject> fixtureWithOneRetry() {

    var retryPolicy =
        new OperationAttempt.RetryPolicy(1, Duration.ofMillis(1)) {
          @Override
          public boolean shouldRetry(Throwable throwable) {
            return true;
          }
        };

    return newFixture(null, retryPolicy, null);
  }

  private OperationAttemptFixture<TestOperationAttempt, TableSchemaObject> newFixture(
      AsyncResultSet resultSet,
      OperationAttempt.RetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory) {

    if (resultSet == null) {
      resultSet = testData.resultSet().emptyResultSet();
    }
    if (retryPolicy == null) {
      retryPolicy = OperationAttempt.RetryPolicy.NO_RETRY;
    }
    if (exceptionHandlerFactory == null) {
      exceptionHandlerFactory = TableDriverExceptionHandler::new;
    }

    var commandExecutor = mock(CommandQueryExecutor.class);

    // spy() the attempt so we get default behaviour and can track calls to the methods
    var attempt =
        spy(
            new TestOperationAttempt(
                0, testData.schemaObject().emptyTableSchemaObject(), retryPolicy, resultSet));

    return new OperationAttemptFixture<>(attempt, commandExecutor, exceptionHandlerFactory, resultSet);
  }
}
