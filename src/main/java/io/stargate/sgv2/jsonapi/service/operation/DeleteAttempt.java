package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to delete a row from a table */
public class DeleteAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<DeleteAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAttempt.class);

  private final WhereCQLClause<Delete> whereCQLClause;

  public DeleteAttempt(int position, SchemaT schemaObject, WhereCQLClause<Delete> whereCQLClause) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);

    // nullable, because the subclass may want to implement method itself.
    // and if there is an error shredding we will not have the insert clause
    this.whereCQLClause = whereCQLClause;
    setStatus(OperationStatus.READY);
  }

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {
    // bind and execute
    var statement = buildDeleteStatement();
    logStatement(LOGGER, "executeStatement()", statement);
    return queryExecutor.executeWrite(statement);
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
