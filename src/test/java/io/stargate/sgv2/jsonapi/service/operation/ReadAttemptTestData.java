package io.stargate.sgv2.jsonapi.service.operation;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableProjection;
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionDefinition;

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

    var objectMapper = new ObjectMapper();

    // spy() the attempt and handler so we get default behaviour and can track calls to the methods
    var attempt =
        spy(
            new TestReadAttempt(
                0,
                testData.schemaObject().emptyTableSchemaObject(),
                testData.selectCQLClause().selectAllFromTable(),
                testData.whereCQLClause().emptySelect(),
                OrderByCqlClause.NO_OP,
                new CQLOptions<>(),
                CqlPagingState.EMPTY,
                TableProjection.fromDefinition(
                    objectMapper,
                    TableProjectionDefinition.createFromDefinition(null),
                    TableSchemaObject.from(testData.tableMetadata().keyValue(), objectMapper)),
                resultSet));

    return new OperationAttemptFixture<>(attempt, queryExecutor, exceptionHandler, resultSet);
  }
}
