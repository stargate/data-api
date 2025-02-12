package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
import io.stargate.sgv2.jsonapi.service.operation.ReadDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link ReadDBTask} to read from a {@link TableSchemaObject}.
 *
 * <p>create a single instance and then call {@link #build(WhereCQLClause)} for each different where
 * clause the command creates.
 */
public class TableReadDBTaskBuilder
    extends TaskBuilder<ReadDBTask<TableSchemaObject>, TableSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadDBTaskBuilder.class);

  private final WhereCQLClauseAnalyzer whereCQLClauseAnalyzer;

  private WithWarnings<SelectCQLClause> selectWithWarnings;
  private WithWarnings<OrderByCqlClause> orderByWithWarnings;
  private WithWarnings<RowSorter> rowSorterWithWarnings;
  private CqlPagingState pagingState = CqlPagingState.EMPTY;
  private final CQLOptions.BuildableCQLOptions<Select> cqlOptions =
      new CQLOptions.BuildableCQLOptions<>();
  private OperationProjection projection;

  public TableReadDBTaskBuilder(TableSchemaObject tableSchemaObject) {
    super(tableSchemaObject);

    this.whereCQLClauseAnalyzer =
        new WhereCQLClauseAnalyzer(tableSchemaObject, WhereCQLClauseAnalyzer.StatementType.SELECT);
  }

  public TableReadDBTaskBuilder withSelect(WithWarnings<SelectCQLClause> selectWithWarnings) {
    this.selectWithWarnings = selectWithWarnings;
    return this;
  }

  public TableReadDBTaskBuilder withProjection(OperationProjection projection) {
    this.projection = projection;
    return this;
  }

  public TableReadDBTaskBuilder withOrderBy(WithWarnings<OrderByCqlClause> orderByWithWarnings) {
    this.orderByWithWarnings = orderByWithWarnings;
    return this;
  }

  public TableReadDBTaskBuilder withSorter(WithWarnings<RowSorter> rowSorterWithWarnings) {
    this.rowSorterWithWarnings = rowSorterWithWarnings;
    return this;
  }

  public TableReadDBTaskBuilder withBuilderOption(CQLOption<Select> option) {
    cqlOptions.addBuilderOption(option);
    return this;
  }

  public TableReadDBTaskBuilder withPagingState(CqlPagingState pagingState) {
    this.pagingState = pagingState;
    return this;
  }

  public ReadDBTask<TableSchemaObject> build(WhereCQLClause<Select> whereCQLClause) {

    Objects.requireNonNull(selectWithWarnings, "selectWithWarnings is required");
    Objects.requireNonNull(orderByWithWarnings, "orderByWithWarnings is required");
    Objects.requireNonNull(rowSorterWithWarnings, "rowSorterWithWarnings is required");
    Objects.requireNonNull(pagingState, "pagingState is required");
    Objects.requireNonNull(projection, "documentSourceSupplier is required");

    WhereCQLClauseAnalyzer.WhereClauseWithWarnings whereWithWarnings = null;
    Exception exception = null;
    try {
      whereWithWarnings = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var atttemptCqlOptions = cqlOptions;
    if (whereWithWarnings != null && whereWithWarnings.requiresAllowFiltering()) {
      // Create a copy of cqlOptions, so we do not impact other tasks
      atttemptCqlOptions = new CQLOptions.BuildableCQLOptions<>(atttemptCqlOptions);
      atttemptCqlOptions.addBuilderOption(CQLOption.ForSelect.allowFiltering());
    }

    var task =
        new ReadDBTask<>(
            nextPosition(),
            schemaObject,
            getExceptionHandlerFactory(),
            selectWithWarnings.target(),
            whereCQLClause,
            orderByWithWarnings.target(),
            atttemptCqlOptions,
            pagingState,
            rowSorterWithWarnings.target(),
            projection);

    // ok to pass null exception, will be ignored
    task.maybeAddFailure(exception);

    // chain up the clauses that may have warnings for the task.
    var warnings = selectWithWarnings.andThen(orderByWithWarnings).andThen(rowSorterWithWarnings);

    if (whereWithWarnings != null) {
      // we have warnings about the where clause, this maybe null if there was an error trying to
      // build the filter
      warnings = warnings.andThen(whereWithWarnings);

      if (LOGGER.isDebugEnabled() && whereWithWarnings.requiresAllowFiltering()) {
        LOGGER.debug(
            "build() - enabled ALLOW FILTERING for attempt {}", task.positionTaskIdStatus());
      }
    }

    // add all the warnings to the task
    warnings.accept(task);
    if (LOGGER.isDebugEnabled() && !task.allWarnings().isEmpty()) {
      LOGGER.debug(
          "build() - adding warnings for task {}, warnings={}",
          task.positionTaskIdStatus(),
          task.allWarnings());
    }
    return task;
  }
}
