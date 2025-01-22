package io.stargate.sgv2.jsonapi.service.operation;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableProjection;

public class ReadAttemptTestData extends OperationAttemptTestData {

  public ReadAttemptTestData(TestData testData) {
    super(testData);
  }

  public OperationAttemptFixture<?, TableSchemaObject> emptyReadAttemptFixture() {
    return newReadAttemptFixture(null, null);
  }

  private OperationAttemptFixture<?, TableSchemaObject> newReadAttemptFixture(
      AsyncResultSet resultSet,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory) {

    if (resultSet == null) {
      resultSet = testData.resultSet().emptyResultSet();
    }

    if (exceptionHandlerFactory == null) {
      exceptionHandlerFactory = TableDriverExceptionHandler::new;
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
                new CQLOptions.BuildableCQLOptions<>(),
                CqlPagingState.EMPTY,
                TableProjection.fromDefinition(
                    objectMapper,
                    mockFindCommand(),
                    TableSchemaObject.from(testData.tableMetadata().keyValue(), objectMapper)),
                resultSet));

    return new OperationAttemptFixture<>(attempt, queryExecutor, exceptionHandlerFactory, resultSet);
  }

  private FindCommand mockFindCommand() {
    var objectMapper = new ObjectMapper();
    String json =
        """
                    {
                      "find": {
                        }
                    }
                  """;
    FindCommand command = null;
    try {
      command = objectMapper.readValue(json, FindCommand.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("fail to deserialize find command " + e);
    }
    return command;
  }
}
