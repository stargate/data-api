package io.stargate.sgv2.jsonapi.service.resolver.sort;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.InMemorySortComparator;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.query.RowSorter;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableRowSorter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Resolves a sort clause to determine expressions to apply for in-memory sorting to the operation.
 */
public class TableMemorySortClauseResolver<CmdT extends Command & Sortable>
    extends TableSortClauseResolver<CmdT, TableSchemaObject, RowSorter> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableMemorySortClauseResolver.class);

  private final OrderByCqlClause orderByCqlClause;
  private final int commandSkip;
  private final int commandLimit;

  /**
   * @param operationsConfig
   * @param orderByCqlClause
   */
  public TableMemorySortClauseResolver(
      OperationsConfig operationsConfig,
      OrderByCqlClause orderByCqlClause,
      int commandSkip,
      int commandLimit) {
    super(operationsConfig);
    this.orderByCqlClause =
        Objects.requireNonNull(orderByCqlClause, "orderByCqlClause must not be null");
    this.commandSkip = commandSkip;
    this.commandLimit = commandLimit;
  }

  @Override
  public WithWarnings<RowSorter> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {
    Objects.requireNonNull(commandContext, "commandContext is required");
    Objects.requireNonNull(command, "command is required");

    if (orderByCqlClause.fullyCoversCommand()) {
      // Cql Order by is enough to handle the sort clause, no need for a row sorter
      // this will also cover where there is no sorting.
      LOGGER.debug("No in memory sort needed, using CQL order by");
      return WithWarnings.of(RowSorter.NO_OP);
    }

    // Just a sanity check,
    var sortClause = command.sortClause();
    if (sortClause == null || sortClause.isEmpty()) {
      LOGGER.debug("No in memory sort needed, sort clause was null or empty");
      return WithWarnings.of(RowSorter.NO_OP);
    }

    // ok, so now we know we need to do an in memory sort, sanity check we should not be here if
    // there are vector sorts
    if (!sortClause.tableVectorSorts().isEmpty()) {
      throw new IllegalStateException("Cannot do table vector sorts in memory");
    }

    return resolveToTableInMemorySort(
        sortClause.nonTableVectorSorts(), commandContext.schemaObject());
  }

  /** Resolves the sort for sorting table rows in memory */
  private WithWarnings<RowSorter> resolveToTableInMemorySort(
      List<SortExpression> sortExpressions, TableSchemaObject tableSchemaObject) {

    // check all the columns are know
    checkUnknownSortColumns(
        tableSchemaObject,
        sortExpressions.stream().map(SortExpression::pathAsCqlIdentifier).toList());

    var apiTableDef = tableSchemaObject.apiTableDef();
    List<InMemorySortComparator.SortByTerm> sortByList = new ArrayList<>(sortExpressions.size());

    sortExpressions.forEach(
        sortExpression -> {
          var apiColumnDef = apiTableDef.allColumns().get(sortExpression.pathAsCqlIdentifier());
          // sanity check, above checked all columns are known
          if (apiColumnDef == null) {
            throw new IllegalStateException(
                "Sort expression not found in table table checking it exists "
                    + sortExpression.pathAsCqlIdentifier());
          }
          sortByList.add(
              new InMemorySortComparator.SortByTerm(apiColumnDef, sortExpression.ascending()));
        });

    var sorter =
        new TableRowSorter(
            new InMemorySortComparator(sortByList),
            commandSkip,
            commandLimit,
            operationsConfig.maxDocumentSortCount() + 1,
            operationsConfig.defaultSortPageSize());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Using in memory sorter: {}", sorter);
    }
    return WithWarnings.of(sorter);
  }
}
