package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.List;
import java.util.Objects;

public class ReadDBTaskAssertions
    extends BaseTaskAssertions<
        ReadDBTaskTestTask, TableSchemaObject, DBTask.AsyncResultSetSupplier, AsyncResultSet> {

  public final ReadDBTaskTestTask readDBTask;
  public final AsyncResultSet resultSet;

  private CommandQueryExecutor mockCommandQueryExecutor;

  public ReadDBTaskAssertions(
      ReadDBTaskTestTask task,
      CommandContext<TableSchemaObject> commandContext,
      AsyncResultSet resultSet) {
    super(task, commandContext);
    this.readDBTask = task;
    this.resultSet = resultSet;

    // set this up because some things will want to get the command query executor and we may want
    // to set assertions
    mockCommandQueryExecutor = mock(CommandQueryExecutor.class);
    doReturn(mockCommandQueryExecutor).when(readDBTask).getCommandQueryExecutor(any());
  }

  /**
   * Make the {@link io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor} the
   * task will use throw the exception first, then return the result set when {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor#executeRead(SimpleStatement)}
   * is called.
   */
  public ReadDBTaskAssertions doExecuteReadThrowThenReturn(
      Throwable exception, AsyncResultSet resultSet) {

    when(mockCommandQueryExecutor.executeRead(any(SimpleStatement.class)))
        .thenThrow(exception)
        .thenReturn(Uni.createFrom().item(resultSet));

    return this;
  }

  /**
   * Verify the task called {@link
   * io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor#executeRead(SimpleStatement)}
   * with each of the CQL statements provided.
   */
  public ReadDBTaskAssertions verifyExecuteReadCql(String msg, String... cql) {

    Objects.requireNonNull(
        mockCommandQueryExecutor,
        "doExecuteReadThrowThenReturn() must be called before verifyExecuteReadCql()");

    var captor = forClass(SimpleStatement.class);
    verify(mockCommandQueryExecutor, times(cql.length)).executeRead(captor.capture());

    List<SimpleStatement> capturedStatements = captor.getAllValues();
    for (int i = 0; i < cql.length; i++) {
      assertThat(capturedStatements.get(i).getQuery())
          .as("CQL Statement for call %s : %s", i, msg)
          .isEqualTo(cql[i]);
    }
    return this;
  }
}
