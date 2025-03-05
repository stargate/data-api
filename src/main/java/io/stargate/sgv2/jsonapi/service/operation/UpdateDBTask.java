package io.stargate.sgv2.jsonapi.service.operation;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to update a row from a table */
public class UpdateDBTask<SchemaT extends TableSchemaObject> extends DBTask<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDBTask.class);

  private final UpdateValuesCQLClause updateCQLClause;
  private final WhereCQLClause<Update> whereCQLClause;

  public UpdateDBTask(
      int position,
      SchemaT schemaObject,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory,
      UpdateValuesCQLClause updateCQLClause,
      WhereCQLClause<Update> whereCQLClause) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);

    // nullable, because the subclass may want to implement method itself.
    // and if there is an error shredding we will not have the insert clause
    this.whereCQLClause = whereCQLClause;
    this.updateCQLClause = updateCQLClause;
    setStatus(TaskStatus.READY);
  }

  public static <SchemaT extends TableSchemaObject> UpdateDBTaskBuilder<SchemaT> builder(
      SchemaT schemaObject) {
    return new UpdateDBTaskBuilder<>(schemaObject);
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildDBResultSupplier(
      CommandContext<SchemaT> commandContext, CommandQueryExecutor queryExecutor) {

    var statement = buildUpdateStatement();

    logStatement(LOGGER, "buildResultSupplier()", statement);
    return new AsyncResultSetSupplier(
        commandContext, this, statement, () -> queryExecutor.executeWrite(statement));
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  /**
   * The framework for WhereCQLClause expects something extending OngoingWhereClause, and there is
   * no easy way to get that for update, we know this will be work DefaultUpdate implements Update -
   * no doing an instanceOf check because of this.
   */
  @SuppressWarnings("unchecked")
  protected static Update uncheckedToUpdate(UpdateWithAssignments updateWithAssignments) {
    return (Update) updateWithAssignments;
  }

  protected SimpleStatement buildUpdateStatement() {
    Objects.requireNonNull(updateCQLClause, "updateCQLClause must not be null");
    Objects.requireNonNull(whereCQLClause, "whereCQLClause must not be null");

    var metadata = schemaObject.tableMetadata();
    // Start the update
    var updateStart = update(metadata.getKeyspace(), metadata.getName());

    // Update the columns
    List<Object> positionalValues = new ArrayList<>();
    var updateWithAssignments = updateCQLClause.apply(updateStart, positionalValues);
    // Add the where clause
    var update = whereCQLClause.apply(uncheckedToUpdate(updateWithAssignments), positionalValues);

    // There are no options for update so far
    return update.build(positionalValues.toArray());
  }
}
