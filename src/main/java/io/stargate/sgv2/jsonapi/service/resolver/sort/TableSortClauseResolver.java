package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableANNOrderByCQlClause;
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
  public OrderByCqlClause resolve(CommandContext<TableSchemaObject> commandContext, CmdT command) {
    Objects.requireNonNull(commandContext, "commandContext is required");
    Objects.requireNonNull(command, "command is required");

    // NOTE: Currently only supporting vector sort, next PR will deal with clustering key sorting
    var apiTableDef = commandContext.schemaObject().apiTableDef();
    var sortClause = command.sortClause();
    if (sortClause == null || sortClause.isEmpty()) {
      return OrderByCqlClause.NO_OP;
    }

    var vectorSorts = sortClause.tableVectorSorts();
    if (vectorSorts.isEmpty()) {
      // For now, only supporting vector sort
      return OrderByCqlClause.NO_OP;
    }

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
    var nonVectorSorts = sortClause.tableNonVectorSorts();
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

    var vectorSort = vectorSorts.getFirst();
    var sortIdentifier = cqlIdentifierFromUserInput(vectorSort.path());
    var apiColumnDef = apiTableDef.allColumns().get(sortIdentifier);
    if (apiColumnDef == null) {
      throw SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put("allColumns", errFmtApiColumnDef(apiTableDef.allColumns()));
                map.put("unknownColumns", errFmt(sortIdentifier));
              }));
    }

    if (apiColumnDef.type().typeName() != ApiTypeName.VECTOR) {
      throw SortException.Code.CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS.get(
          errVars(
              commandContext.schemaObject(),
              map -> {
                map.put(
                    "vectorColumns",
                    errFmtApiColumnDef(apiTableDef.allColumns().filterBy(ApiTypeName.VECTOR)));
                map.put("sortColumns", errFmt(sortIdentifier));
              }));
    }

    // HACK - waiting for index support on the APiTableDef
    var optionalIndexMetadata = findIndexMetadata(commandContext.schemaObject(), apiColumnDef);
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
                map.put("sortColumns", errFmt(sortIdentifier));
              }));
    }

    // This is a bit of a hack, we should be using the codecs to convert but for now the Sort
    // deserialization
    // turns the JSON array into a float array, so we can just use that.
    // Needs more refactoring to change how it works
    var cqlVector = CqlVectorUtil.floatsToCqlVector(vectorSort.vector());

    return new TableANNOrderByCQlClause(apiColumnDef, cqlVector);
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
