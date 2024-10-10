package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSourceSupplier;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttempt;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
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

  private CqlPagingState pagingState = CqlPagingState.EMPTY;
  private final CqlOptions<Select> cqlOptions = new CqlOptions<>();

  public TableReadAttemptBuilder(
      TableSchemaObject tableSchemaObject,
      SelectCQLClause selectCQLClause,
      DocumentSourceSupplier documentSourceSupplier) {

    this.tableSchemaObject = tableSchemaObject;
    this.selectCQLClause = selectCQLClause;
    this.documentSourceSupplier = documentSourceSupplier;
    this.whereCQLClauseAnalyzer = new WhereCQLClauseAnalyzer(tableSchemaObject);
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

    WhereCQLClauseAnalyzer.WhereClauseAnalysis analyzedResult = null;
    Exception exception = null;
    try {
      analyzedResult = whereCQLClauseAnalyzer.analyse(whereCQLClause);
    } catch (FilterException filterException) {
      exception = filterException;
    }

    var atttemptCqlOptions = cqlOptions;
    if (analyzedResult != null && analyzedResult.requiresAllowFiltering()) {
      // Create a copy of cqlOptions, so we do not impact other attempts
      atttemptCqlOptions = new CqlOptions<>(atttemptCqlOptions);
      atttemptCqlOptions.addBuilderOption(CQLOption.ForSelect.withAllowFiltering());
    }

    var tableReadAttempt =
        new ReadAttempt<>(
            readPosition,
            tableSchemaObject,
            selectCQLClause,
            whereCQLClause,
            atttemptCqlOptions,
            pagingState,
            documentSourceSupplier);

    // ok to pass null exception, will be ignored
    tableReadAttempt.maybeAddFailure(exception);

    if (analyzedResult != null) {
      if (LOGGER.isDebugEnabled()) {
        if (analyzedResult.requiresAllowFiltering()) {
          LOGGER.debug(
              "build() - enabled ALLOW FILTERING for attempt {}",
              tableReadAttempt.positionAndAttemptId());
        }
        if (!analyzedResult.warningExceptions().isEmpty()) {
          LOGGER.debug(
              "build() - adding warnings for attempt {}, warnings={}",
              tableReadAttempt.positionAndAttemptId(),
              analyzedResult.warningExceptions());
        }
      }

      analyzedResult.warningExceptions().forEach(tableReadAttempt::addWarning);
    }

    return tableReadAttempt;
  }
}
