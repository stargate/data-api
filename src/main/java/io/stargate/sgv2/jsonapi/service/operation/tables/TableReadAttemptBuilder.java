package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
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

/**
 * Builds an attempt to read a row from an API Table, create a single instance and then call {@link
 * #build(WhereCQLClause)} for each different where clause the command creates.
 *
 * <p>Note: we don't need a subclass for ReadAttempt, everything is on the superclass
 */
public class TableReadAttemptBuilder implements ReadAttemptBuilder<ReadAttempt<TableSchemaObject>> {

  // first value is zero, but we increment before we use it
  private int readPosition = -1;

  private final TableSchemaObject tableSchemaObject;
  private final SelectCQLClause selectCQLClause;
  private CqlPagingState pagingState = CqlPagingState.EMPTY;
  private final DocumentSourceSupplier documentSourceSupplier;

  private final CqlOptions<Select> cqlOptions = new CqlOptions<>();

  public TableReadAttemptBuilder(
      TableSchemaObject tableSchemaObject,
      SelectCQLClause selectCQLClause,
      DocumentSourceSupplier documentSourceSupplier) {

    this.tableSchemaObject = tableSchemaObject;
    this.selectCQLClause = selectCQLClause;
    this.documentSourceSupplier = documentSourceSupplier;
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

    final WhereCQLClauseAnalyzer.WhereCQLClauseAnalyzeResult whereCQLClauseAnalyzeResult =
        whereCQLClause.analyseWhereClause();
    // Analyse and may add AllowFiltering
    if (whereCQLClauseAnalyzeResult.withAllowFiltering()) {
      cqlOptions.addBuilderOption(CQLOption.ForSelect.withAllowFiltering());
    }

    var tableReadAttempt =
        new ReadAttempt<>(
            readPosition,
            tableSchemaObject,
            selectCQLClause,
            whereCQLClause,
            cqlOptions,
            pagingState,
            documentSourceSupplier);

    // Analyse and may add warning
    whereCQLClauseAnalyzeResult.warnings().forEach(tableReadAttempt::addWarning);

    return tableReadAttempt;
  }
}
