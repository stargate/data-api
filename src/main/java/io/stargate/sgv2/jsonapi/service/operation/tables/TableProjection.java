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
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.*;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionSelector;
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionSelectors;
import io.stargate.sgv2.jsonapi.service.projection.TableUDTProjectionSelector;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import java.util.*;
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

  private ObjectMapper objectMapper;
  private TableSchemaObject table;

  /**
   * The columns selected at the top level, based on the projection definition. This is a subset of
   * all table columns.
   */
  private List<ColumnMetadata> selectedColumns;

  /**
   * Selectors for precise selection of scalar column and UDT subfields. Since we get the top level
   * values from selected {@link #selectedColumns}, we need to use the selectors to do 1. precise
   * projection, column level or selected UDT subfields level. 2. precise schema description, column
   * level or selected UDT subfields level.
   */
  private TableProjectionSelectors preciseSelectors;

  private TableSimilarityFunction tableSimilarityFunction;

  private TableProjection(
      ObjectMapper objectMapper,
      TableSchemaObject table,
      List<ColumnMetadata> selectedColumns,
      TableSimilarityFunction tableSimilarityFunction,
      TableProjectionSelectors preciseSelectors) {

    this.objectMapper = objectMapper;
    this.table = table;
    this.selectedColumns = selectedColumns;
    this.tableSimilarityFunction = tableSimilarityFunction;
    this.preciseSelectors = preciseSelectors;
  }

  /**
   * Factory method for construction projection instance, given a projection definition and table
   * schema.
   */
  public static <CmdT extends Projectable> TableProjection fromDefinition(
      CommandContext<TableSchemaObject> ctx, ObjectMapper objectMapper, CmdT command) {

    TableSchemaObject table = ctx.schemaObject();

    // Build projectionSelectors first
    var projectionSelectors =
        TableProjectionSelectors.from(command.tableProjectionDefinition(), table);

    // Get column metadata map
    Map<String, ColumnMetadata> columnsByName = new HashMap<>();
    table
        .tableMetadata()
        .getColumns()
        .forEach((id, column) -> columnsByName.put(id.asInternal(), column));

    // Then compute selected topLevel selectedColumns based on inclusion/exclusion mode
    List<ColumnMetadata> selectedColumns = projectionSelectors.toCqlColumns();

    // TODO: A table can't be with empty selectedColumns. Think a redundant check.
    if (selectedColumns.isEmpty()) {
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

    TableProjection projection =
        new TableProjection(
            objectMapper,
            table,
            selectedColumns,
            TableSimilarityFunction.from(ctx, command),
            projectionSelectors);

    return projection;
  }

  @Override
  public Select apply(OngoingSelection ongoingSelection) {
    Set<CqlIdentifier> readColumns = new LinkedHashSet<>();
    readColumns.addAll(selectedColumns.stream().map(ColumnMetadata::getName).toList());
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
    for (int i = 0, len = selectedColumns.size(); i < len; ++i) {
      final ColumnMetadata column = selectedColumns.get(i);
      final String columnName = column.getName().asInternal();
      JSONCodec codec;

      // TODO: maybe optimize common case of String, Boolean to avoid conversions, lookups
      try {
        codec = JSONCodecRegistries.DEFAULT_REGISTRY.codecToJSON(table.tableMetadata(), column);
      } catch (MissingJSONCodecException e) {
        throw ErrorCodeV1.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            "Column '%s' has unsupported type '%s'", columnName, column.getType().toString());
      }
      try {
        final Object columnValue = row.getObject(i);
        // By default, null value will not be returned.
        // https://github.com/stargate/data-api/issues/1636 issue for adding nullOption
        switch (columnValue) {
          case null -> skippedNullCount++;
          case Collection<?> collection when collection.isEmpty() ->
              // For set/list/map values, java driver wrap up as empty Collection/Map, Data API only
              // returns non-sparse data currently.
              skippedNullCount++;
          case Map<?, ?> map when map.isEmpty() -> skippedNullCount++;
          default -> {
            nonNullCount++;
            JsonNode projectedValue = projectColumnValue(column, columnValue, codec);
            if (projectedValue != null) {
              result.set(columnName, projectedValue);
            }
          }
        }
      } catch (ToJSONCodecException e) {
        throw ErrorCodeV1.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            e,
            "Column '%s' has invalid value of type '%s': failed to convert to JSON: %s",
            columnName,
            column.getType().toString(),
            e.getMessage());
      }
    }

    if (LOGGER.isDebugEnabled()) {
      double durationMs = (System.nanoTime() - startNano) / 1_000_000.0;
      LOGGER.debug(
          "projectRow() row build durationMs={}, columns.size={}, nonNullCount={}, skippedNullCount={}",
          durationMs,
          selectedColumns.size(),
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

  /**
   * Projects a column value based on the configured selectors.
   *
   * <p>This method handles both simple column values and complex UDT values with subfield
   * projections.
   *
   * @param column the column metadata
   * @param columnValue the raw column value from the database row
   * @param rootCodec the JSON codec for converting the root column value
   * @return the projected JSON value, or null if the column should be excluded entirely
   */
  private JsonNode projectColumnValue(
      ColumnMetadata column, Object columnValue, JSONCodec rootCodec) throws ToJSONCodecException {

    // Find selector that applies to this root column
    TableProjectionSelector targetSelector =
        preciseSelectors.getSelectorForColumn(column.getName());

    JsonNode fullProjectionNode = rootCodec.toJSON(objectMapper, columnValue);
    if (fullProjectionNode == null) return null;

    return targetSelector.projectToJsonNode(fullProjectionNode);
  }

  @Override
  public ColumnsDescContainer getSchemaDescription() {
    // Build projected schema directly from selectors
    return buildProjectionSchema();
  }

  /**
   * Build projection schema directly from selectors. For non-UDT columns, include the whole column
   * schema. For UDT columns with subfield selections, include only the selected field schema.
   */
  private ColumnsDescContainer buildProjectionSchema() {

    ColumnsDescContainer projectedSchemaDesc = new ColumnsDescContainer();

    // Build schema for each selected column based on its selector
    for (var selector : preciseSelectors.getSelectors().values()) {
      CqlIdentifier columnIdentifier = selector.getColumnIdentifier();

      if (selector.isProjectOnUDTColumn()) {
        var udtSelector = (TableUDTProjectionSelector) selector;
        var udtApiType = (ApiUdtType) udtSelector.getColumnDef().type();
        projectedSchemaDesc.put(
            columnIdentifier.asInternal(),
            udtApiType.projectedSchemaDescription(
                SchemaDescSource.DML_USAGE, udtSelector.getSelectedUDTFields()));
      } else {
        // Non-UDT column - include the whole column schema
        var columnDesc = selector.getColumnDef().getSchemaDescription(SchemaDescSource.DML_USAGE);
        if (columnDesc != null) {
          projectedSchemaDesc.put(selector.getColumnIdentifier(), columnDesc);
        }
      }
    }
    return projectedSchemaDesc;
  }

  public List<ColumnMetadata> getSelectedColumns() {
    return selectedColumns;
  }
}
