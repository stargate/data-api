package io.stargate.sgv2.jsonapi.service.operation;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;

public class ReadAttemptTestData extends OperationAttemptTestData {

  public ReadAttemptTestData(TestData testData) {
    super(testData);
  }

  public OperationAttemptFixture<?, TableSchemaObject> emptyReadAttemptFixture() {
    return newReadAttemptFixture(null, null);
  }

  private OperationAttemptFixture<?, TableSchemaObject> newReadAttemptFixture(
      AsyncResultSet resultSet, TableDriverExceptionHandler exceptionHandler) {

    if (resultSet == null) {
      resultSet = testData.resultSet().emptyResultSet();
    }

    if (exceptionHandler == null) {
      exceptionHandler = spy(new TableDriverExceptionHandler());
    }

    var queryExecutor = mock(CommandQueryExecutor.class);

    // spy() the attempt and handler so we get default behaviour and can track calls to the methods
    var attempt =
        spy(
            new TestReadAttempt(
                0,
                testData.schemaObject().emptyTableSchemaObject(),
                testData.selectCQLClause().selectAllFromTable(),
                testData.whereCQLClause().emptySelect(),
                OrderByCqlClause.NO_OP,
                new CqlOptions<>(),
                CqlPagingState.EMPTY,
                null,
                resultSet));

    return new OperationAttemptFixture<>(attempt, queryExecutor, exceptionHandler, resultSet);
  }
}
