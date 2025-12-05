package io.stargate.sgv2.jsonapi.service.operation;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task to read from a CQL table, runs the query, holds the result set, and then builds the
 * documents on demand.
 */
public class ReadDBTask<SchemaT extends TableBasedSchemaObject> extends DBTask<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadDBTask.class);

  private final SelectCQLClause selectCQLClause;
  private final WhereCQLClause<Select> whereCQLClause;
  private final OrderByCqlClause orderByCqlClause;
  private final CQLOptions<Select> cqlOptions;
  private final CqlPagingState pagingState;
  private final RowSorter rowSorter;
  private final OperationProjection projection;

  // Need to have this downcast reference so we can call read specific methods
  private ReadAttemptRetryPolicy<SchemaT> readAttemptRetryPolicy;
  private ReadResult readResult;

  public ReadDBTask(
      int position,
      SchemaT schemaObject,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory,
      SelectCQLClause selectCQLClause,
      WhereCQLClause<Select> whereCQLClause,
      OrderByCqlClause orderByCqlClause,
      CQLOptions<Select> cqlOptions,
      CqlPagingState pagingState,
      RowSorter rowSorter,
      OperationProjection projection) {
    super(position, schemaObject, new ReadAttemptRetryPolicy<SchemaT>(), exceptionHandlerFactory);

    // nullable because the subclass may want to implement methods to build the statement itself
    this.selectCQLClause = selectCQLClause;
    this.whereCQLClause = whereCQLClause;
    this.orderByCqlClause = orderByCqlClause;
    this.cqlOptions = cqlOptions;
    this.pagingState = pagingState;
    this.projection = Objects.requireNonNull(projection, "projection must not be null");
    this.rowSorter = Objects.requireNonNull(rowSorter, "rowSorter must not be null");

    downcastRetryPolicy();
    Objects.requireNonNull(readAttemptRetryPolicy, "readAttemptRetryPolicy must not be null");
    setStatus(TaskStatus.READY);
  }

  @SuppressWarnings("unchecked")
  private void downcastRetryPolicy() {
    readAttemptRetryPolicy = (ReadAttemptRetryPolicy<SchemaT>) retryPolicy;
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildDBResultSupplier(
      CommandContext<SchemaT> commandContext, CommandQueryExecutor queryExecutor) {

    var statement = buildReadStatement();

    // TODO: aaron feb 4, we no longer need this special try policy if the BaseTask passed the
    // ResultSupplier to
    // the retry policy
    readAttemptRetryPolicy.setRetryContext(
        new ReadAttemptRetryPolicy.RetryContext<>(statement, this));

    logStatement(LOGGER, "buildResultSupplier()", statement);
    return new AsyncResultSetSupplier(
        commandContext, this, statement, () -> rowSorter.executeRead(queryExecutor, statement));
  }

  /** {@inheritDoc} */
  @Override
  public void onSuccess(AsyncResultSetSupplier resultSetSupplier, AsyncResultSet result) {
    readResult = new ReadResult(rowSorter, result);

    // call to make sure status is set
    super.onSuccess(resultSetSupplier, result);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<ColumnsDescContainer> schemaDescription() {

    // need to check because otherwise we do not have the read result
    if (!checkStatus("schemaDescription()", TaskStatus.COMPLETED)) {
      return Optional.empty();
    }
    return Optional.of(projection.getSchemaDescription());
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  /**
   * Get the documents from the result set, the documents are not created until this method is
   * called.
   *
   * @return List of JsonNode documents, never null. The documents are created using the {@link
   *     OperationProjection}.
   */
  public List<JsonNode> documents() {

    // we must be terminal, but that does not mean we have a result set
    assertTerminalStatus("documents()");

    List<JsonNode> documents = new ArrayList<>();
    if (readResult != null) {
      readResult.currentPage.forEach(row -> documents.add(projection.projectRow(row)));
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
    assertTerminalStatus("resultPagingState()");
    return readResult == null ? CqlPagingState.EMPTY : readResult.pagingState;
  }

  /**
   * Gets the total number of rows that were sorted, that is all the rows that were read from the
   * database.
   *
   * @return
   */
  public Optional<Integer> sortedRowCount() {
    return rowSorter.sortedRowCount();
  }

  protected SimpleStatement buildReadStatement() {

    List<Object> positionalValues = new ArrayList<>();

    // Note, use ExtendedSelect to support AND/OR in where clause, see details in
    // ExtendedSelect.java.
    var selectFrom = new DefaultSelect(schemaObject.keyspaceName(), schemaObject.tableName());
    var select = applySelect(selectFrom, positionalValues);
    // these are options that go on the query builder, such as limit or allow filtering
    var bindableQuery = applyOptions(select);
    var statement = bindableQuery.build(positionalValues.toArray());
    // these are options that go on the statement, such as page size
    statement = applyOptions(statement);

    return rowSorter.updatePagingState(pagingState).addToStatement(statement);
  }

  protected Select applySelect(SelectFrom selectFrom, List<Object> positionalValues) {
    Objects.requireNonNull(selectCQLClause, "selectFrom must not be null");
    Objects.requireNonNull(whereCQLClause, "whereCQLClause must not be null");
    Objects.requireNonNull(orderByCqlClause, "orderByCqlClause must not be null");

    // Add the columns we want to select
    Select select = selectCQLClause.apply(selectFrom);
    // Row sorting may need to add columns to the select to do sorting by
    select = rowSorter.addToSelect(select);
    // Add the where clause
    select = whereCQLClause.apply(select, positionalValues);
    // and finally order by
    // 14-Jul-2025, tatu: NOTE! ORDER BY _MUST_ be added after the where clause,
    // otherwise we will get UnsupportedOperationException (if different ordering
    // of construction needed, must change "LexicalSortSelect" to allow that).
    select = orderByCqlClause.apply(select);

    return select;
  }

  protected BuildableQuery applyOptions(Select select) {
    // The sorter may need to update the options that are applied, e.g. to change the limit or page
    // size
    return rowSorter.updateCqlOptions(cqlOptions).applyBuilderOptions(select);
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

    ReadResult(RowSorter rowSorter, AsyncResultSet resultSet) {
      this.resultSet = Objects.requireNonNull(resultSet, "resultSet must not be null");
      this.currentPage = resultSet.currentPage();
      this.pagingState = rowSorter.buildPagingState(resultSet);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "ReadResult() created resultSet.remaining: {}, resultSet.hasMorePages={}, (api) pagingState:{}",
            resultSet.remaining(),
            resultSet.hasMorePages(),
            pagingState);
      }
    }
  }

  /**
   * Retry policy that will retry a read attempt if the query fails due to missing Allow Filtering
   *
   * @param <T>
   */
  static class ReadAttemptRetryPolicy<T extends TableBasedSchemaObject> extends TaskRetryPolicy {

    private RetryContext<T> retryContext = null;

    ReadAttemptRetryPolicy() {
      super(1, Duration.ofMillis(1));
    }

    void setRetryContext(RetryContext<T> retryContext) {
      this.retryContext = retryContext;
    }

    @Override
    public boolean shouldRetry(Throwable throwable) {

      // retryContext could be null, this is when cql read statement build has an exception
      if (retryContext == null) {
        return false;
      }
      // clear the retry context so that we don't keep a reference to the last statement
      var currentRetryContext = retryContext;
      retryContext = null;

      var allowFilteringMissing =
          throwable instanceof InvalidQueryException
              && throwable.getMessage().contains("use ALLOW FILTERING");

      if (allowFilteringMissing) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Retrying read attempt with added ALLOW FILTERING for {}, original query cql={} , values={}",
              currentRetryContext.task.taskDesc(),
              currentRetryContext.lastStatement.getQuery(),
              currentRetryContext.lastStatement.getPositionalValues());
        }

        currentRetryContext.task.addWarning(
            WarningException.Code.QUERY_RETRIED_DUE_TO_INDEXING.get(
                errVars(
                    currentRetryContext.task.schemaObject,
                    map -> {
                      map.put("originalCql", currentRetryContext.lastStatement.getQuery());
                      map.put(
                          "originalParameters",
                          currentRetryContext.lastStatement.getPositionalValues().toString());
                    })));
        currentRetryContext.task.cqlOptions.addBuilderOption(CQLOption.ForSelect.allowFiltering());
        return true;
      }
      return false;
    }

    record RetryContext<T extends TableBasedSchemaObject>(
        SimpleStatement lastStatement, ReadDBTask<T> task) {}
  }
}
