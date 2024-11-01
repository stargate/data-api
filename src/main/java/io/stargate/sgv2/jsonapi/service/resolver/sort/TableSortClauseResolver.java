package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableANNOrderByCQlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableClusteringOrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a sort clause to deterimine if we want to apply a CQL ORDER BY clause to the operation.
 *
 * <p>Not all sort clauses result in a CQL order by, some may result in memory sorting.
 */
public class TableSortClauseResolver<CmdT extends Command & Sortable>
    extends SortClauseResolver<CmdT, TableSchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSortClauseResolver.class);

  public TableSortClauseResolver(
      OperationsConfig operationsConfig, JSONCodecRegistry codecRegistry) {
    super(operationsConfig);
  }

  @Override
  public WithWarnings<OrderByCqlClause> resolve(
      CommandContext<TableSchemaObject> commandContext, CmdT command) {
    Objects.requireNonNull(commandContext, "commandContext is required");
    Objects.requireNonNull(command, "command is required");

    // NOTE: Currently only supporting vector sort, next PR will deal with clustering key sorting
    var apiTableDef = commandContext.schemaObject().apiTableDef();
    var sortClause = command.sortClause();
    if (sortClause == null || sortClause.isEmpty()) {
      LOGGER.debug("Sort clause is null or empty, no CQL ORDER BY needed.");
      return WithWarnings.of(OrderByCqlClause.NO_OP);
    }

    // All sorting can only be on columns in the table definition
    var sortColumns = sortClause.sortColumnIdentifiers();
    checkUnknownSortColumns(commandContext.schemaObject(), sortColumns);

    var vectorSorts = sortClause.tableVectorSorts();
    return vectorSorts.isEmpty()
        ? resolveNonVectorSort(commandContext, sortClause, sortColumns)
        : resolveVectorSort(commandContext, sortClause, vectorSorts);
  }

  /**
   * We have atleast one sort expression, and non of them are vector sorts.
   *
   * <p>If the sort uses the clustering keys in the correct way according to CQL, then we can use
   * the CQL Order By to push the sorting to the database. See {@link
   * TableClusteringOrderByCqlClause}. If not then we return a {@link OrderByCqlClause#NO_OP} to
   * indicate that we cannot push the sorting to the database and need to do it in memory.
   */
  private WithWarnings<OrderByCqlClause> resolveNonVectorSort(
      CommandContext<TableSchemaObject> commandContext,
      SortClause sortClause,
      List<CqlIdentifier> sortColumns) {

    var apiTableDef = commandContext.schemaObject().apiTableDef();

    // If there is any sorting on non partition sorting columns, we cannot use CQL ORDER BY
    var nonClusteringKeySorts =
        sortColumns.stream()
            .filter(sortColumn -> !apiTableDef.clusteringKeys().containsKey(sortColumn))
            .sorted(CQL_IDENTIFIER_COMPARATOR)
            .toList();
    if (!nonClusteringKeySorts.isEmpty()) {
      var warn =
          WarningException.Code.IN_MEMORY_SORTING_DUE_TO_NON_PARTITION_SORTING.get(
              errVars(
                  commandContext.schemaObject(),
                  map -> {
                    map.put(
                        "partitionSorting",
                        errFmtApiColumnDef(apiTableDef.clusteringKeys().values()));
                    map.put("sortColumns", errFmtCqlIdentifier(nonClusteringKeySorts));
                  }));

      return WithWarnings.of(OrderByCqlClause.NO_OP, warn);
    }

    // We know all of the order keys are partition sorting keys
    // if the order of the sort columns is not the same as the clustering keys, we cannot use CQL
    // this covers two cases: if the clustering is [a,b,c] covers
    // -  [a,c] where b is missed
    // - [a,c,b] where the order is wrong
    boolean isOutOfOrder = false;
    var clusteringKeys = apiTableDef.clusteringKeys().identifiers();
    for (int i = 0; i < sortColumns.size(); i++) {
      if (!sortColumns.get(i).equals(clusteringKeys.get(i))) {
        isOutOfOrder = true;
        break;
      }
    }
    if (isOutOfOrder) {
      var warn =
          WarningException.Code.IN_MEMORY_SORTING_DUE_TO_OUT_OF_ORDER_PARTITION_SORTING.get(
              errVars(
                  commandContext.schemaObject(),
                  map -> {
                    map.put(
                        "partitionSorting",
                        errFmtApiColumnDef(apiTableDef.clusteringKeys().values()));
                    map.put(
                        "sortColumns",
                        errFmtCqlIdentifier(sortColumns)); // no not change the order of the sorting
                  }));

      return WithWarnings.of(OrderByCqlClause.NO_OP, warn);
    }

    var orderByTerms =
        sortClause.sortExpressions().stream()
            .map(
                sortExpression -> {
                  var apiColumnDef =
                      apiTableDef.allColumns().get(sortExpression.pathAsCqlIdentifier());
                  return new TableClusteringOrderByCqlClause.OrderByTerm(
                      apiColumnDef,
                      sortExpression.ascending()
                          ? TableClusteringOrderByCqlClause.Order.ASC
                          : TableClusteringOrderByCqlClause.Order.DESC);
                })
            .toList();
    var cqlOrderBy = new TableClusteringOrderByCqlClause(orderByTerms);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Sort covered by clustering keys, using CQL ORDER BY. cqlOrderBy: {}", cqlOrderBy);
    }
    return WithWarnings.of(cqlOrderBy);
  }

  /**
   * We have at least one vector sort in the sort clause.
   *
   * <p>This is always implemented by using a CQL Order By to push the ANN search to the database.
   * See {@link TableANNOrderByCQlClause}
   */
  private WithWarnings<OrderByCqlClause> resolveVectorSort(
      CommandContext<TableSchemaObject> commandContext,
      SortClause sortClause,
      List<SortExpression> vectorSorts) {

    var apiTableDef = commandContext.schemaObject().apiTableDef();

    if (vectorSorts.size() > 1) {
      throw SortException.Code.MORE_THAN_ONE_VECTOR_SORT.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR)));
                map.put(
                    "sortColumns",
                    errFmtJoin(vectorSorts.stream().map(SortExpression::path).toList()));
              }));
    }

    // we have one vector sort - cannot have any other sorting
    var nonVectorSorts = sortClause.nonTableVectorSorts();
    if (!nonVectorSorts.isEmpty()) {
      throw SortException.Code.CANNOT_MIX_VECTOR_AND_NON_VECTOR_SORT.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR)));
                map.put(
                    "sortVectorColumns",
                    errFmtJoin(vectorSorts.stream().map(SortExpression::path).toList()));
                map.put(
                    "sortNonVectorColumns",
                    errFmtJoin(nonVectorSorts.stream().map(SortExpression::path).toList()));
              }));
    }

    var vectorSortExpression = vectorSorts.getFirst();
    var vectorSortIdentifier = vectorSortExpression.pathAsCqlIdentifier();
    var vectorSortColumn = apiTableDef.allColumns().get(vectorSortIdentifier);

    if (vectorSortColumn.type().typeName() != ApiTypeName.VECTOR) {
      throw SortException.Code.CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR)));
                map.put("sortColumns", errFmt(vectorSortIdentifier));
              }));
    }

    // HACK - waiting for index support on the APiTableDef
    var optionalIndexMetadata = findIndexMetadata(commandContext.schemaObject(), vectorSortColumn);
    if (optionalIndexMetadata.isEmpty()) {
      throw SortException.Code.CANNOT_VECTOR_SORT_NON_INDEXED_VECTOR_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR)));
                map.put(
                    "indexedColumns",
                    errFmtJoin(indexedVectorColumns(commandContext.schemaObject())));
                map.put("sortColumns", errFmt(vectorSortIdentifier));
              }));
    }

    // This is a bit of a hack, we should be using the codecs to convert but for now the Sort
    // deserialization
    // turns the JSON array into a float array, so we can just use that.
    // Needs more refactoring to change how it works
    LOGGER.debug(
        "Vector sorting on column {}", cqlIdentifierToMessageString(vectorSortColumn.name()));
    var cqlVector = CqlVectorUtil.floatsToCqlVector(vectorSortExpression.vector());
    return WithWarnings.of(new TableANNOrderByCQlClause(vectorSortColumn, cqlVector));
  }

  private void checkUnknownSortColumns(
      TableSchemaObject tableSchemaObject, List<CqlIdentifier> sortColumns) {

    var unknownColumns =
        sortColumns.stream()
            .filter(
                sortIdentifier ->
                    !tableSchemaObject.apiTableDef().allColumns().containsKey(sortIdentifier))
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

  private Optional<IndexMetadata> findIndexMetadata(
      TableSchemaObject schemaObject, ApiColumnDef targetColumn) {
    return schemaObject.tableMetadata().getIndexes().values().stream()
        .filter(index -> index.getTarget().equals(targetColumn.name().asInternal()))
        .findFirst();
  }

  private List<String> indexedVectorColumns(TableSchemaObject schemaObject) {

    var apiVectorColumns = schemaObject.apiTableDef().allColumns().filterBy(ApiTypeName.VECTOR);
    return schemaObject.tableMetadata().getIndexes().values().stream()
        .map(IndexMetadata::getTarget)
        .filter(target -> apiVectorColumns.containsKey(CqlIdentifier.fromInternal(target)))
        .toList();
  }
}
