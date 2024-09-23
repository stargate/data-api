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

public class ReadAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<ReadAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadAttempt.class);

  private final SelectCQLClause selectCQLClause;
  private final WhereCQLClause<Select> whereCQLClause;
  private final CqlOptions<Select> cqlOptions;
  private final CqlPagingState pagingState;
  private DocumentSourceSupplier documentSourceSupplier;

  private ReadResult readResult;

  protected ReadAttempt(
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
  }

  // TODO: AARON this is ugly, just making it work first
  public List<JsonNode> documents() {

    checkStatus("documents()", OperationStatus.COMPLETED);

    List<JsonNode> documents = new ArrayList<>();
    readResult.currentPage.forEach(
        row -> documents.add(documentSourceSupplier.documentSource(row).get()));

    return documents;
  }

  public CqlPagingState resultPagingState() {
    return readResult == null ? CqlPagingState.EMPTY : readResult.pagingState;
  }

  @Override
  protected Uni<AsyncResultSet> execute(CommandQueryExecutor queryExecutor) {

    var statement = buildReadStatement();

    LOGGER.warn("FIND CQL: {}", statement.getQuery());
    LOGGER.warn("FIND VALUES: {}", statement.getPositionalValues());

    return queryExecutor.executeRead(statement);
  }

  @Override
  protected ReadAttempt<SchemaT> onSuccess(AsyncResultSet resultSet) {
    readResult = new ReadResult(resultSet);
    return super.onSuccess(resultSet);
  }

  protected SimpleStatement buildReadStatement() {

    var metadata = schemaObject.tableMetadata();
    List<Object> positionalValues = new ArrayList<>();

    var selectFrom = selectFrom(metadata.getKeyspace(), metadata.getName());
    var select = applySelect(selectFrom, positionalValues);
    var bindableQuery = applyOptions(select);
    var statement = bindableQuery.build(positionalValues.toArray());
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
