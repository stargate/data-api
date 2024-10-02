package io.stargate.sgv2.jsonapi.service.operation;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An attempt to read from a table, runs the query, holds the result set, and then builds the
 * documents on demand.
 */
public class ReadAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<ReadAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadAttempt.class);

  private final SelectCQLClause selectCQLClause;
  private final WhereCQLClause<Select> whereCQLClause;
  private final CqlOptions<Select> cqlOptions;
  private final CqlPagingState pagingState;
  // DocumentSourceSupplier is an old idea that needs refactoring at some point, is a supplier of a
  // supplier
  // but this class encapsulates the idea of how to convert a row into a document.
  private final DocumentSourceSupplier documentSourceSupplier;

  private ReadResult readResult;

  public ReadAttempt(
      int position,
      SchemaT schemaObject,
      SelectCQLClause selectCQLClause,
      WhereCQLClause<Select> whereCQLClause,
      CqlOptions<Select> cqlOptions,
      CqlPagingState pagingState,
      DocumentSourceSupplier documentSourceSupplier) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);

    // nullable because the subclass may want to implement methods to build the statement itself
    this.selectCQLClause = selectCQLClause;
    this.whereCQLClause = whereCQLClause;
    this.cqlOptions = cqlOptions;
    this.pagingState = pagingState;
    this.documentSourceSupplier = documentSourceSupplier;

    setStatus(OperationStatus.READY);
  }

  /**
   * Get the documents from the result set, the documents are not created until this method is
   * called.
   *
   * @return List of JsonNode documents, never null.
   */
  public List<JsonNode> documents() {

    // we must be terminal, but that does not mean we have a result set
    checkTerminal("documents()");

    List<JsonNode> documents = new ArrayList<>();
    if (readResult != null) {
      readResult.currentPage.forEach(
          row -> documents.add(documentSourceSupplier.documentSource(row).get()));
    }
    return documents;
  }

  /**
   * Get the paging state form running this command.
   *
   * @return {@link CqlPagingState} which is never null, if the statement did not have a paging
   *     state then {@link CqlPagingState#EMPTY} is returned (or if the attempt is terminal but
   *     never run the statement)
   */
  public CqlPagingState resultPagingState() {
    // we must be terminal, but that does not mean we have a result set
    checkTerminal("resultPagingState()");
    return readResult == null ? CqlPagingState.EMPTY : readResult.pagingState;
  }

  @Override
  protected Uni<AsyncResultSet> execute(CommandQueryExecutor queryExecutor) {

    var statement = buildReadStatement();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "execute() - {}, cql={}, values={}",
          positionAndAttemptId(),
          statement.getQuery(),
          statement.getPositionalValues());
    }
    return queryExecutor.executeRead(statement);
  }

  @Override
  protected ReadAttempt<SchemaT> onSuccess(AsyncResultSet resultSet) {
    readResult = new ReadResult(resultSet);
    // call to make sure status is set
    return super.onSuccess(resultSet);
  }

  protected SimpleStatement buildReadStatement() {

    var metadata = schemaObject.tableMetadata();
    List<Object> positionalValues = new ArrayList<>();

    var selectFrom = selectFrom(metadata.getKeyspace(), metadata.getName());
    var select = applySelect(selectFrom, positionalValues);
    // these are options that go on the query builder, such as limit or allow filtering
    var bindableQuery = applyOptions(select);
    var statement = bindableQuery.build(positionalValues.toArray());
    // these are options that go on the statement, such as page size
    statement = applyOptions(statement);

    return pagingState.addToStatement(statement);
  }

  protected Select applySelect(SelectFrom selectFrom, List<Object> positionalValues) {
    Objects.requireNonNull(selectCQLClause, "selectFrom must not be null");
    Objects.requireNonNull(whereCQLClause, "whereCQLClause must not be null");

    // Add the columns we want to select
    Select select = selectCQLClause.apply(selectFrom);

    // Add the where clause
    select = whereCQLClause.apply(select, positionalValues);
    return select;
  }

  protected BuildableQuery applyOptions(Select select) {
    return cqlOptions.applyBuilderOptions(select);
  }

  protected SimpleStatement applyOptions(SimpleStatement statement) {
    return cqlOptions.applyStatementOptions(statement);
  }

  // This is a simple container for the result set so we can set one variable in the onSuccess
  // method
  static class ReadResult {

    final AsyncResultSet resultSet;
    final Iterable<Row> currentPage;
    final CqlPagingState pagingState;

    ReadResult(AsyncResultSet resultSet) {
      this.resultSet = Objects.requireNonNull(resultSet, "resultSet must not be null");
      this.currentPage = resultSet.currentPage();
      this.pagingState = CqlPagingState.from(resultSet);
    }
  }
}
