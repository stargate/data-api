package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSourceSupplier;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.Objects;

public class TableReadAttemptBuilder implements ReadAttemptBuilder<TableReadAttempt> {

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
  public TableReadAttempt build(WhereCQLClause<Select> whereCQLClause) {

    readPosition += 1;

    return new TableReadAttempt(
        tableSchemaObject,
        readPosition,
        selectCQLClause,
        whereCQLClause,
        cqlOptions,
        pagingState,
        documentSourceSupplier);
  }
}
