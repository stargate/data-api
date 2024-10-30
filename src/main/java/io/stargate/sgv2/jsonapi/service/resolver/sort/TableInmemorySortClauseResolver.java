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
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInmemorySortClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableInmemorySortClauseResolver<CmdT extends Command & Sortable>
    extends InMemorySortClauseResolver<CmdT, TableSchemaObject> {

  public TableInmemorySortClauseResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  public TableInmemorySortClause resolve(
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
  private TableInmemorySortClause resolveToTableInMemorySort(
      List<SortExpression> inMemorySorts,
      TableSchemaObject tableSchemaObject,
      ApiTableDef apiTableDef) {
    List<TableInmemorySortClause.OrderBy> orderByList = new ArrayList<>();
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
          new TableInmemorySortClause.OrderBy(apiColumnDef, sortExpression.ascending()));
    }
    return new TableInmemorySortClause(orderByList);
  }
}
