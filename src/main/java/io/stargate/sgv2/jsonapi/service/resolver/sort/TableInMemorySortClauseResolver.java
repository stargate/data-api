package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInMemorySortClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/*
 * Resolves a sort clause to determine expressions to apply for in-memory sorting to the operation.
 */
public class TableInMemorySortClauseResolver<CmdT extends Command & Sortable>
    extends InMemorySortClauseResolver<CmdT, TableSchemaObject> {

  public TableInMemorySortClauseResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  public TableInMemorySortClause resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {
    Objects.requireNonNull(commandContext, "commandContext is required");
    Objects.requireNonNull(command, "command is required");

    var apiTableDef = commandContext.schemaObject().apiTableDef();
    var sortClause = command.sortClause();
    if (sortClause == null || sortClause.isEmpty()) {
      return null;
    }
    var inmemorySorts = sortClause.tableNonVectorSorts();
    return resolveToTableInMemorySort(inmemorySorts, commandContext.schemaObject(), apiTableDef);
  }

  // Verify and resolve to a TableInMemoryOrderByCQLClause
  private TableInMemorySortClause resolveToTableInMemorySort(
      List<SortExpression> inMemorySorts,
      TableSchemaObject tableSchemaObject,
      ApiTableDef apiTableDef) {
    List<TableInMemorySortClause.OrderBy> orderByList = new ArrayList<>();
    for (SortExpression sortExpression : inMemorySorts) {
      var sortIdentifier = cqlIdentifierFromUserInput(sortExpression.path());
      var apiColumnDef = apiTableDef.allColumns().get(sortIdentifier);
      if (apiColumnDef == null) {
        throw SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns()));
                  map.put("unknownColumns", errFmt(sortIdentifier));
                }));
      }
      orderByList.add(
          new TableInMemorySortClause.OrderBy(apiColumnDef, sortExpression.ascending()));
    }
    return new TableInMemorySortClause(orderByList);
  }
}
