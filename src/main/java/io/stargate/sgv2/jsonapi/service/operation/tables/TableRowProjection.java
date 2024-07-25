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
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionDefinition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Projection used for Table Rows (as opposed to Collection Documents), built from command API
 * projection definitions (expressed in JSON).
 */
public record TableRowProjection(ObjectMapper objectMapper, List<ColumnMetadata> columns)
    implements OperationProjection {
  /**
   * Factory method for construction projection instance, given a projection definition and table
   * schema.
   */
  public static TableRowProjection fromDefinition(
      ObjectMapper objectMapper,
      TableProjectionDefinition projectionDefinition,
      TableSchemaObject schema) {
    Map<String, ColumnMetadata> columnsByName = new HashMap<>();
    schema
        .tableMetadata
        .getColumns()
        .forEach((id, column) -> columnsByName.put(id.asInternal(), column));

    List<ColumnMetadata> columns = projectionDefinition.extractSelectedColumns(columnsByName);

    if (columns.isEmpty()) {
      throw ErrorCode.UNSUPPORTED_PROJECTION_DEFINITION.toApiException(
          "did not include any Table columns");
    }

    return new TableRowProjection(objectMapper, columns);
  }

  @Override
  public Select forSelect(SelectFrom selectFrom) {
    return selectFrom.columnsIds(columns.stream().map(ColumnMetadata::getName).toList());
  }

  @Override
  public DocumentSource toDocument(Row row) {
    ObjectNode result = objectMapper.createObjectNode();
    for (int i = 0, len = columns.size(); i < len; ++i) {
      ColumnMetadata column = columns.get(i);
      // result.put(column.getName().asInternal(), row.getString(i));
      // !!! TODO
      result.put(
          column.getName().asInternal(),
          column.getType().toString() + "/" + column.getType().getClass().getName());
    }
    return () -> result;
  }
}
