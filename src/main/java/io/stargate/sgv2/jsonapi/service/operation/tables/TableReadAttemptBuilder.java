package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.RowsContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.SortedRowsContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSourceSupplier;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttempt;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds an attempt to read a row from an API Table, create a single instance and then call {@link
 * #build(WhereCQLClause)} for each different where clause the command creates.
 *
 * <p>Note: we don't need a subclass for ReadAttempt, everything is on the superclass
 */
public class TableReadAttemptBuilder implements ReadAttemptBuilder<ReadAttempt<TableSchemaObject>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadAttemptBuilder.class);

  // first value is zero, but we increment before we use it
  private int readPosition = -1;

  private final TableSchemaObject tableSchemaObject;
  private final SelectCQLClause selectCQLClause;
  private final DocumentSourceSupplier documentSourceSupplier;
  private final WhereCQLClauseAnalyzer whereCQLClauseAnalyzer;
  private final WithWarnings<OrderByCqlClause> orderByCqlClause;

  private CqlPagingState pagingState = CqlPagingState.EMPTY;
  private final TableInMemorySortClause tableInmemorySortClause;
  private final InMemorySortOption inMemorySortOption;
  private final CqlOptions<Select> cqlOptions = new CqlOptions<>();

  public TableReadAttemptBuilder(
      TableSchemaObject tableSchemaObject,
      SelectCQLClause selectCQLClause,
      DocumentSourceSupplier documentSourceSupplier,
      WithWarnings<OrderByCqlClause> orderByCqlClause,
      TableInMemorySortClause tableInmemorySortClause,
      InMemorySortOption inMemorySortOption) {

    this.tableSchemaObject = tableSchemaObject;
    this.selectCQLClause = selectCQLClause;
    this.documentSourceSupplier = documentSourceSupplier;
    this.whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(tableSchemaObject, WhereCQLClauseAnalyzer.StatementType.SELECT);
    this.orderByCqlClause = orderByCqlClause;
    this.tableInmemorySortClause = tableInmemorySortClause;
    this.inMemorySortOption = inMemorySortOption;
  }

  public TableReadAttemptBuilder addBuilderOption(CQLOption<Select> option) {
    cqlOptions.addBuilderOption(option);
    return this;
  }

  public TableReadAttemptBuilder addStatementOption(CQLOption<SimpleStatement> option) {
    cqlOptions.addStatementOption(option);
    return this;
  }

  public TableReadAttemptBuilder addPagingState(CqlPagingState pagingState) {
    this.pagingState = Objects.requireNonNull(pagingState, "pagingState must not be null");
    return this;
  }

  @Override
  public ReadAttempt<TableSchemaObject> build(WhereCQLClause<Select> whereCQLClause) {

    readPosition += 1;

    WhereCQLClauseAnalyzer.WhereClauseWithWarnings whereClauseWithWarnings = null;
    Exception exception = null;
    try {
      whereClauseWithWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var atttemptCqlOptions = cqlOptions;
    if (whereClauseWithWarnings != null && whereClauseWithWarnings.requiresAllowFiltering()) {
      // Create a copy of cqlOptions, so we do not impact other attempts
      atttemptCqlOptions = new CqlOptions<>(atttemptCqlOptions);
      atttemptCqlOptions.addBuilderOption(CQLOption.ForSelect.allowFiltering());
    }
    var rowsContainer =
        tableInmemorySortClause != null
            ? new SortedRowsContainer(inMemorySortOption, tableInmemorySortClause)
            : RowsContainer.defaultRowsContainer();

    ReadAttempt tableReadAttempt =
        new ReadAttempt<>(
            readPosition,
            tableSchemaObject,
            selectCQLClause,
            whereCQLClause,
            orderByCqlClause.target(),
            atttemptCqlOptions,
            pagingState,
            documentSourceSupplier,
            rowsContainer);

    // ok to pass null exception, will be ignored
    tableReadAttempt.maybeAddFailure(exception);

    if (whereClauseWithWarnings != null) {
      if (LOGGER.isDebugEnabled()) {
        if (whereClauseWithWarnings.requiresAllowFiltering()) {
          LOGGER.debug(
              "build() - enabled ALLOW FILTERING for attempt {}",
              tableReadAttempt.positionAndAttemptId());
        }
        if (!whereClauseWithWarnings.isEmpty()) {
          LOGGER.debug(
              "build() - adding warnings for attempt {}, warnings={}",
              tableReadAttempt.positionAndAttemptId(),
              whereClauseWithWarnings.warnings());
        }
      }

      whereClauseWithWarnings.accept(tableReadAttempt);
    }

    orderByCqlClause.accept(tableReadAttempt);
    return tableReadAttempt;
  }
}
