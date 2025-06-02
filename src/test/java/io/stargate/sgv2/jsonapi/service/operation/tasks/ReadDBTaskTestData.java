package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.querybuilder.select.OngoingSelection;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableProjection;
import java.util.List;

public class ReadDBTaskTestData {
  private static final Tenant TENANT = Tenant.create(DatabaseType.ASTRA, "test-tenant");

  public ReadDBTaskTestData() {}

  public ReadDBTaskAssertions defaultTask(String keyspaceName, String tableName) {
    return createAssertions(keyspaceName, tableName, null, null);
  }

  private ReadDBTaskAssertions createAssertions(
      String keyspaceName,
      String tableName,
      AsyncResultSet resultSet,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory) {

    if (resultSet == null) {
      resultSet = mock(AsyncResultSet.class);

      // this is how we show there is no CQL tracing
      var executionInfo = mock(ExecutionInfo.class);
      when(executionInfo.getTracingId()).thenReturn(null);
      when(resultSet.getExecutionInfo()).thenReturn(executionInfo);
    }

    if (exceptionHandlerFactory == null) {
      exceptionHandlerFactory = TableDriverExceptionHandler::new;
    }

    var mockTable = BaseTaskAssertions.mockTable(TENANT, keyspaceName, tableName);
    CommandContext<TableSchemaObject> mockCommandContext = mock(CommandContext.class);
    when(mockCommandContext.requestTracing()).thenReturn(RequestTracing.NO_OP);

    var mockProjection = mock(TableProjection.class);

    // spy() the attempt and handler so we get default behaviour and can track calls to the methods
    var task =
        spy(
            new ReadDBTaskTestTask(
                0,
                mockTable,
                exceptionHandlerFactory,
                OngoingSelection::all,
                emptySelect(),
                OrderByCqlClause.NO_OP,
                new CQLOptions.BuildableCQLOptions<>(),
                CqlPagingState.EMPTY,
                mockProjection,
                resultSet));

    return new ReadDBTaskAssertions(task, mockCommandContext, resultSet);
  }

  public WhereCQLClause<Select> emptySelect() {
    return new WhereCQLClause<>() {
      @Override
      public Select apply(Select select, List<Object> objects) {
        return select;
      }

      @Override
      public DBLogicalExpression getLogicalExpression() {
        return null;
      }

      @Override
      public boolean selectsSinglePartition(TableSchemaObject tableSchemaObject) {
        throw new IllegalArgumentException(
            "WhereCQLClauseTestData does not support partitionKeysFullyRestrictedByEq");
      }
    };
  }
}
