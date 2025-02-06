package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.Task;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to truncate a table */
public class TruncateDBTask<SchemaT extends TableBasedSchemaObject>
    extends DBTask<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TruncateDBTask.class);

  public TruncateDBTask(int position,
                        SchemaT schemaObject,
                        DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);

    setStatus(TaskStatus.READY);
  }

  public static <SchemaT extends TableBasedSchemaObject> TaskBuilder.BasicTaskBuilder<TruncateDBTask<SchemaT>, SchemaT> builder(
      SchemaT schemaObject) {
    return new TaskBuilder.BasicTaskBuilder<TruncateDBTask<SchemaT>, SchemaT>(schemaObject, TruncateDBTask::new);
  }


  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildResultSupplier(CommandQueryExecutor queryExecutor) {

    var statement = buildTruncateStatement();

    logStatement(LOGGER, "buildResultSupplier()", statement);
    return new AsyncResultSetSupplier(statement, () -> queryExecutor.executeTruncate(statement));
  }

  // =================================================================================================
  // Internal
  // =================================================================================================

  protected SimpleStatement buildTruncateStatement() {

    var metadata = schemaObject.tableMetadata();
    return QueryBuilder.truncate(metadata.getKeyspace(), metadata.getName()).build();
  }
}
