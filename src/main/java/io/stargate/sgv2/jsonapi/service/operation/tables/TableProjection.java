package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.select.OngoingSelection;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.*;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The projection for a read operation.
 *
 * <p>This class does double duty: it implements the {@link SelectCQLClause} interface to apply the
 * projection to the cql statement, and it implements the {@link OperationProjection} interface to
 * apply the projection to rows read from the database.
 *
 * <p>TODO: refactor to use the factory / builder pattern for other clauses and give better errors
 */
public class TableProjection implements SelectCQLClause, OperationProjection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableProjection.class);

  /**
   * Match if a column does not support reads so we can find unsupported columns from the
   * projection.
   */
  private static final Predicate<ApiSupportDef> MATCH_READ_UNSUPPORTED =
      ApiSupportDef.Matcher.NO_MATCHES.withRead(false);

  private ObjectMapper objectMapper;
  private TableSchemaObject table;
  private List<ColumnMetadata> columns;
  private ColumnsDescContainer columnsDesc;
  private TableSimilarityFunction tableSimilarityFunction;

  private TableProjection(
      ObjectMapper objectMapper,
      TableSchemaObject table,
      List<ColumnMetadata> columns,
      ColumnsDescContainer columnsDesc,
      TableSimilarityFunction tableSimilarityFunction) {

    this.objectMapper = objectMapper;
    this.table = table;
    this.columns = columns;
    this.columnsDesc = columnsDesc;
    this.tableSimilarityFunction = tableSimilarityFunction;
  }

  /**
   * Factory method for construction projection instance, given a projection definition and table
   * schema.
   */
  public static <CmdT extends Projectable> TableProjection fromDefinition(
      CommandContext<TableSchemaObject> ctx, ObjectMapper objectMapper, CmdT command) {

    TableSchemaObject table = ctx.schemaObject();
    Map<String, ColumnMetadata> columnsByName = new HashMap<>();
    // TODO: This can also be cached as part of TableSchemaObject than resolving it for every query.
    table
        .tableMetadata()
        .getColumns()
        .forEach((id, column) -> columnsByName.put(id.asInternal(), column));

    List<ColumnMetadata> columns =
        command.tableProjectionDefinition().extractSelectedColumns(columnsByName);

    // TODO: A table can't be with empty columns. Think a redundant check.
    if (columns.isEmpty()) {
      throw ProjectionException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              table,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(table.apiTableDef().allColumns()));
                map.put(
                    "unknownColumns",
                    command.tableProjectionDefinition().getColumnNames().toString());
              }));
    }

    // result set has ColumnDefinitions not ColumnMetadata kind of weird

    var readApiColumns =
        table
            .apiTableDef()
            .allColumns()
            .filterByIdentifiers(columns.stream().map(ColumnMetadata::getName).toList());

    var unsupportedColumns = readApiColumns.filterBySupportToList(MATCH_READ_UNSUPPORTED);
    if (!unsupportedColumns.isEmpty()) {
      throw ProjectionException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          errVars(
              table,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(table.apiTableDef().allColumns()));
                map.put("unsupportedColumns", errFmtApiColumnDef(unsupportedColumns));
              }));
    }

    return new TableProjection(
        objectMapper,
        table,
        columns,
        readApiColumns.getSchemaDescription(SchemaDescSource.DML_USAGE),
        TableSimilarityFunction.from(ctx, command));
  }

  @Override
  public Select apply(OngoingSelection ongoingSelection) {
    Set<CqlIdentifier> readColumns = new LinkedHashSet<>();
    readColumns.addAll(columns.stream().map(ColumnMetadata::getName).toList());
    Select select = ongoingSelection.columnsIds(readColumns);

    // may apply similarity score function
    return tableSimilarityFunction.apply(select);
  }

  @Override
  public JsonNode projectRow(Row row) {
    long startNano = System.nanoTime();
    int nonNullCount = 0;
    int skippedNullCount = 0;

    ObjectNode result = objectMapper.createObjectNode();
    for (int i = 0, len = columns.size(); i < len; ++i) {
      final ColumnMetadata column = columns.get(i);
      final String columnName = column.getName().asInternal();
      JSONCodec codec;

      // TODO: maybe optimize common case of String, Boolean to avoid conversions, lookups
      try {
        codec = JSONCodecRegistries.DEFAULT_REGISTRY.codecToJSON(table.tableMetadata(), column);
      } catch (MissingJSONCodecException e) {
        throw ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM.get(
            Map.of(
                "errorMessage",
                "column '%s' has unsupported type '%s'"
                    .formatted(columnName, column.getType().toString())));
      }
      try {
        final Object columnValue = row.getObject(i);
        // By default, null value will not be returned.
        // https://github.com/stargate/data-api/issues/1636 issue for adding nullOption
        switch (columnValue) {
          case null -> {
            skippedNullCount++;
          }
            // For set/list/map values, java driver wrap up as empty Collection/Map, Data API only
            // returns non-sparse data currently.
          case Collection<?> collection when collection.isEmpty() -> {
            skippedNullCount++;
          }
          case Map<?, ?> map when map.isEmpty() -> {
            skippedNullCount++;
          }
          default -> {
            nonNullCount++;
            result.set(columnName, codec.toJSON(objectMapper, columnValue));
          }
        }

      } catch (ToJSONCodecException e) {
        throw ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM.get(
            Map.of(
                "errorMessage",
                "column '%s' has invalid value of type '%s'; failed to convert to JSON: %s."
                    .formatted(columnName, column.getType().toString(), e.getMessage())));
      }
    }

    if (LOGGER.isDebugEnabled()) {
      double durationMs = (System.nanoTime() - startNano) / 1_000_000.0;
      LOGGER.debug(
          "projectRow() row build durationMs={}, columns.size={}, nonNullCount={}, skippedNullCount={}",
          durationMs,
          columns.size(),
          nonNullCount,
          skippedNullCount);
    }

    // If user specify includeSimilarity, but no ANN sort clause, then we won't generate
    // similarity_score function in the cql statement
    if (tableSimilarityFunction.canProjectSimilarity()) {
      try {
        final float aFloat = row.getFloat(TableSimilarityFunction.SIMILARITY_SCORE_ALIAS);
        result.put(VECTOR_FUNCTION_SIMILARITY_FIELD, aFloat);
        // Should not happen, but keep it caught, in case it breaks the query
      } catch (IllegalArgumentException ignored) {
      }
    }
    return result;
  }

  @Override
  public ColumnsDescContainer getSchemaDescription() {
    return columnsDesc;
  }
}
