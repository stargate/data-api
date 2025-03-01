package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to delete a row from a table */
public class DeleteDBTask<SchemaT extends TableBasedSchemaObject> extends DBTask<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteDBTask.class);

  private final WhereCQLClause<Delete> whereCQLClause;

  public DeleteDBTask(
      int position,
      SchemaT schemaObject,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory,
      WhereCQLClause<Delete> whereCQLClause) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);

    // nullable, because the subclass may want to implement method itself.
    // and if there is an error shredding we will not have the insert clause
    this.whereCQLClause = whereCQLClause;
    setStatus(TaskStatus.READY);
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildDBResultSupplier(
      CommandContext<SchemaT> commandContext, CommandQueryExecutor queryExecutor) {

    var statement = buildDeleteStatement();

    logStatement(LOGGER, "buildResultSupplier()", statement);
    return new AsyncResultSetSupplier(
        commandContext, statement, () -> queryExecutor.executeWrite(statement));
  }

  /**
   * The framework for WhereCQLClause expects something extending OngoingWhereClause, and there is
   * no easy way to get that for delete, we know this will be work DefaultDelete implements
   * DefaultDelete - no doing an instanceOf check because of this.
   */
  @SuppressWarnings("unchecked")
  protected static Delete uncheckedToDelete(DeleteSelection deleteSelection) {
    return (Delete) deleteSelection;
  }

  protected SimpleStatement buildDeleteStatement() {
    Objects.requireNonNull(whereCQLClause, "whereCQLClause must not be null");

    var metadata = schemaObject.tableMetadata();

    var delete =
        uncheckedToDelete(QueryBuilder.deleteFrom(metadata.getKeyspace(), metadata.getName()));

    List<Object> positionalValues = new ArrayList<>();
    // Add the where clause
    delete = whereCQLClause.apply(delete, positionalValues);
    // There are no options for delete so far
    return delete.build(positionalValues.toArray());
  }
}
