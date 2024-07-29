package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSource;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.ToJSONCodecException;
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionDefinition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Projection used for Table Rows (as opposed to Collection Documents), built from command API
 * projection definitions (expressed in JSON).
 */
public record TableRowProjection(
    ObjectMapper objectMapper, TableSchemaObject table, List<ColumnMetadata> columns)
    implements OperationProjection {
  /**
   * Factory method for construction projection instance, given a projection definition and table
   * schema.
   */
  public static TableRowProjection fromDefinition(
      ObjectMapper objectMapper,
      TableProjectionDefinition projectionDefinition,
      TableSchemaObject table) {
    Map<String, ColumnMetadata> columnsByName = new HashMap<>();
    // TODO: This can also be cached as part of TableSchemaObject than resolving it for every query.
    table
        .tableMetadata
        .getColumns()
        .forEach((id, column) -> columnsByName.put(id.asInternal(), column));

    List<ColumnMetadata> columns = projectionDefinition.extractSelectedColumns(columnsByName);

    // TODO: A table can't be with empty columns. Think a redundant check.
    if (columns.isEmpty()) {
      throw ErrorCode.UNSUPPORTED_PROJECTION_DEFINITION.toApiException(
          "did not include any Table columns");
    }

    return new TableRowProjection(objectMapper, table, columns);
  }

  @Override
  public Select forSelect(SelectFrom selectFrom) {
    return selectFrom.columnsIds(columns.stream().map(ColumnMetadata::getName).toList());
  }

  @Override
  public DocumentSource toDocument(Row row) {
    ObjectNode result = objectMapper.createObjectNode();
    for (int i = 0, len = columns.size(); i < len; ++i) {
      final ColumnMetadata column = columns.get(i);
      final String columnName = column.getName().asInternal();
      JSONCodec codec;

      // TODO: maybe optimize common case of String, Boolean to avoid conversions, lookups
      try {
        codec = JSONCodecRegistry.codecToJSON(table.tableMetadata, column);
      } catch (MissingJSONCodecException e) {
        throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            "Column '%s' has unsupported type '%s'", columnName, column.getType().toString());
      }
      try {
        result.put(columnName, codec.toJSON(objectMapper, row.getObject(i)));
      } catch (ToJSONCodecException e) {
        throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            e,
            "Column '%s' has invalid value of type '%s': failed to convert to JSON: %s",
            columnName,
            column.getType().toString(),
            e.getMessage());
      }
    }
    return () -> result;
  }
}
