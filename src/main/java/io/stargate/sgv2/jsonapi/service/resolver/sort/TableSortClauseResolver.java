package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;
import java.util.List;

/**
 * Common base for common code when resolving the sort clause for either CQL or in memory sorting
 *
 * @param <CmdT> The command type
 * @param <SchemaT> The schema object type
 * @param <ReturnT> The type that the resolver returns, e.g. in memory or cql sorting
 */
public abstract class TableSortClauseResolver<
        CmdT extends Command & Sortable, SchemaT extends SchemaObject, ReturnT>
    extends ClauseResolver<CmdT, SchemaT, ReturnT> {

  protected TableSortClauseResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  /**
   * Checks that all the sort columns are known columns in the table.
   *
   * @param tableSchemaObject The table schema object to check
   * @param sortColumns The sort columns to check
   * @throws {@link SortException} of {@link SortException.Code#CANNOT_SORT_UNKNOWN_COLUMNS} if any
   *     are not present
   */
  protected void checkUnknownSortColumns(
      TableSchemaObject tableSchemaObject, List<CqlIdentifier> sortColumns) {

    var unknownColumns =
        sortColumns.stream()
            .filter(
                sortIdentifier ->
                    !tableSchemaObject.apiTableDef().allColumns().containsKey(sortIdentifier))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();

    if (!unknownColumns.isEmpty()) {
      throw SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns", errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumns));
              }));
    }
  }
}
