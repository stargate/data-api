package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.InsertValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IMPORTANT: THIS IS ALSO USED BY THE COLLECTIONS (JUST FOR INSERT) SO IT NEEDS TO STAY UNTIL
 * COLLECTIONS CODE IS UPDATED (INSERTS STARTED THE "ATTEMPT" PATTERN)
 */
public abstract class InsertAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<InsertAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertAttempt.class);

  private final InsertValuesCQLClause insertValuesCQLClause;

  protected InsertAttempt(
      int position, SchemaT schemaObject, InsertValuesCQLClause insertValuesCQLClause) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);

    // nullable, because the subclass may want to implement method itself.
    // and if there is an error shredding we will not have the insert clause
    this.insertValuesCQLClause = insertValuesCQLClause;
  }

  @Override
  protected StatementContext buildStatementContext(CommandQueryExecutor queryExecutor) {
    // bind and execute
    var statement = buildInsertStatement();

    logStatement(LOGGER, "executeStatement()", statement);
    return new StatementContext(statement, () -> queryExecutor.executeWrite(statement));
  }

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
