package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
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
 * Container for an individual Document or Row insertion attempt.
 *
 * <p>Tracks the original input position; document (if available), its id (if available) and
 * possible processing error.
 *
 * <p>Information will be needed to build responses, including the optional detail response see
 * {@link InsertOperationPage}
 *
 * <p>Is {@link Comparable} so that the attempts can be re-sorted into the order provided in the
 * user request, compares based on the {@link #position()}
 */
public abstract class InsertAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<InsertAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertAttempt.class);
  private final InsertValuesCQLClause insertValuesCQLClause;

  protected InsertAttempt(
      int position, SchemaT schemaObject, InsertValuesCQLClause insertValuesCQLClause) {
    super(position, schemaObject);
    // nullable, because the subclass may want to implement method itself.
    this.insertValuesCQLClause = insertValuesCQLClause;
  }

  @Override
  protected Uni<AsyncResultSet> execute(CommandQueryExecutor queryExecutor) {
    // bind and execute
    var boundStatement = buildInsertStatement();

    LOGGER.warn("INSERT CQL: {}", boundStatement.getQuery());
    LOGGER.warn("INSERT VALUES: {}", boundStatement.getPositionalValues());

    return queryExecutor.executeWrite(boundStatement);
  }

  @Override
  protected InsertAttempt<SchemaT> onSuccess(AsyncResultSet resultSet) {
    setStatus(OperationStatus.COMPLETED);
    return this;
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

  /**
   * Called to get the description of the schema to use when building the response.
   *
   * @return The optional object that describes the schema, if present the object will be serialised
   *     to JSON and included in the response status as {@link CommandStatus#PRIMARY_KEY_SCHEMA}.
   */
  public abstract Optional<Object> schemaDescription();
}
