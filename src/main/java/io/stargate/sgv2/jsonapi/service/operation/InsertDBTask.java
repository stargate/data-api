package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;import io.stargate.sgv2.jsonapi.service.operation.query.InsertValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to insert a single row in CQL.
 *
 * <p>IMPORTANT: this is for tables only for now, see {@link InsertAttempt} we need to keep it
 * around as well.
 */
public abstract class InsertDBTask<SchemaT extends TableBasedSchemaObject> extends DBTask<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertDBTask.class);

  private final InsertValuesCQLClause insertValuesCQLClause;

  protected InsertDBTask(
      int position,
      SchemaT schemaObject,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory,
      InsertValuesCQLClause insertValuesCQLClause) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);

    // nullable, because the subclass may want to implement method itself.
    // and if there is an error shredding we will not have the insert clause
    this.insertValuesCQLClause = insertValuesCQLClause;
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildDBResultSupplier(
      CommandContext<SchemaT> commandContext, CommandQueryExecutor queryExecutor) {

    var statement = buildInsertStatement();

    logStatement(LOGGER, "buildResultSupplier()", statement);
    return new AsyncResultSetSupplier(
        commandContext, this, statement, () -> queryExecutor.executeWrite(statement));
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  protected SimpleStatement buildInsertStatement() {

    var metadata = schemaObject.tableMetadata();

    InsertInto insertInto = QueryBuilder.insertInto(metadata.getKeyspace(), metadata.getName());

    List<Object> positionalValues = new ArrayList<>();
    RegularInsert regularInsert = applyInsertValues(insertInto, positionalValues);
    return SimpleStatement.newInstance(regularInsert.asCql(), positionalValues.toArray());
  }

  protected RegularInsert applyInsertValues(
      OngoingValues ongoingValues, List<Object> positionalValues) {
    Objects.requireNonNull(insertValuesCQLClause, "insertValuesCQLClause must not be null");
    return insertValuesCQLClause.apply(ongoingValues, positionalValues);
  }

  /**
   * The document _id or the row primary key, if known, used to build the response that includes the
   * Id's of the documents / rows that were successfully inserted or failed.
   *
   * <p>Optional as there may be times when the input document / row could not be parsed to get the
   * ID. And separate to having the doc / row shreddded because we may have the id (such as when
   * creating a new document _id sever side) but were not able to shred the document / row.
   *
   * @return The {@link DocRowIdentifer} that identifies the document or row by ID
   */
  public abstract Optional<DocRowIdentifer> docRowID();
}
