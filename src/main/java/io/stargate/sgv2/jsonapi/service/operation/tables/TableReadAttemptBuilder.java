package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
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
  private final WhereCQLClauseAnalyzer whereCQLClauseAnalyzer;

  private WithWarnings<SelectCQLClause> selectWithWarnings;
  private WithWarnings<OrderByCqlClause> orderByWithWarnings;
  private WithWarnings<RowSorter> rowSorterWithWarnings;
  private CqlPagingState pagingState = CqlPagingState.EMPTY;
  private final CQLOptions.BuildableCQLOptions<Select> cqlOptions =
      new CQLOptions.BuildableCQLOptions<>();

  private OperationProjection projection;

  public TableReadAttemptBuilder(TableSchemaObject tableSchemaObject) {

    this.tableSchemaObject = tableSchemaObject;
    this.whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(tableSchemaObject, WhereCQLClauseAnalyzer.StatementType.SELECT);
  }

  public TableReadAttemptBuilder addSelect(WithWarnings<SelectCQLClause> selectWithWarnings) {
    this.selectWithWarnings = selectWithWarnings;
    return this;
  }

  public TableReadAttemptBuilder addOrderBy(WithWarnings<OrderByCqlClause> orderByWithWarnings) {
    this.orderByWithWarnings = orderByWithWarnings;
    return this;
  }

  public TableReadAttemptBuilder addSorter(WithWarnings<RowSorter> rowSorterWithWarnings) {
    this.rowSorterWithWarnings = rowSorterWithWarnings;
    return this;
  }

  public TableReadAttemptBuilder addBuilderOption(CQLOption<Select> option) {
    cqlOptions.addBuilderOption(option);
    return this;
  }

  public TableReadAttemptBuilder addPagingState(CqlPagingState pagingState) {
    this.pagingState = pagingState;
    return this;
  }

  public TableReadAttemptBuilder addProjection(OperationProjection projection) {
    this.projection = projection;
    return this;
  }

  @Override
  public ReadAttempt<TableSchemaObject> build(WhereCQLClause<Select> whereCQLClause) {
    Objects.requireNonNull(selectWithWarnings, "selectWithWarnings is required");
    Objects.requireNonNull(orderByWithWarnings, "orderByWithWarnings is required");
    Objects.requireNonNull(rowSorterWithWarnings, "rowSorterWithWarnings is required");
    Objects.requireNonNull(pagingState, "pagingState is required");
    Objects.requireNonNull(projection, "documentSourceSupplier is required");

    readPosition += 1;

    WhereCQLClauseAnalyzer.WhereClauseWithWarnings whereWithWarnings = null;
    Exception exception = null;
    try {
      whereWithWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var atttemptCqlOptions = cqlOptions;
    if (whereWithWarnings != null && whereWithWarnings.requiresAllowFiltering()) {
      // Create a copy of cqlOptions, so we do not impact other attempts
      atttemptCqlOptions = new CQLOptions.BuildableCQLOptions<>(atttemptCqlOptions);
      atttemptCqlOptions.addBuilderOption(CQLOption.ForSelect.allowFiltering());
    }

    var tableReadAttempt =
        new ReadAttempt<>(
            readPosition,
            tableSchemaObject,
            selectWithWarnings.target(),
            whereCQLClause,
            orderByWithWarnings.target(),
            atttemptCqlOptions,
            pagingState,
            rowSorterWithWarnings.target(),
            projection);

    // ok to pass null exception, will be ignored
    tableReadAttempt.maybeAddFailure(exception);

    // chain up the clauses that may have warnings for the attempt.
    var attemptConsumers =
        selectWithWarnings.andThen(orderByWithWarnings).andThen(rowSorterWithWarnings);

    if (whereWithWarnings != null) {
      attemptConsumers = attemptConsumers.andThen(whereWithWarnings);

      if (LOGGER.isDebugEnabled() && whereWithWarnings.requiresAllowFiltering()) {
        LOGGER.debug(
            "build() - enabled ALLOW FILTERING for attempt {}",
            tableReadAttempt.positionAndAttemptId());
      }
    }

    attemptConsumers.accept(tableReadAttempt);
    if (LOGGER.isDebugEnabled() && !tableReadAttempt.warnings().isEmpty()) {
      LOGGER.debug(
          "build() - adding warnings for attempt {}, warnings={}",
          tableReadAttempt.positionAndAttemptId(),
          tableReadAttempt.warnings());
    }
    return tableReadAttempt;
  }
}
