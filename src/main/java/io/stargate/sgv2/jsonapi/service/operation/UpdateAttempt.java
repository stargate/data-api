package io.stargate.sgv2.jsonapi.service.operation;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to update a row from a table */
public class UpdateAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<UpdateAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateAttempt.class);

  private final UpdateValuesCQLClause updateCQLClause;
  private final WhereCQLClause<Update> whereCQLClause;

  public UpdateAttempt(
      int position,
      SchemaT schemaObject,
      UpdateValuesCQLClause updateCQLClause,
      WhereCQLClause<Update> whereCQLClause) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);

    // nullable, because the subclass may want to implement method itself.
    // and if there is an error shredding we will not have the insert clause
    this.whereCQLClause = whereCQLClause;
    this.updateCQLClause = updateCQLClause;
    setStatus(OperationStatus.READY);
  }

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {
    // bind and execute
    var statement = buildUpdateStatement();
    logStatement(LOGGER, "executeStatement()", statement);
    return queryExecutor.executeWrite(statement);
  }

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
