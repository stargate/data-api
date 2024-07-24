package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSource;
import java.util.List;

public record SomeJSONProjection(ObjectMapper objectMapper, List<ColumnMetadata> columns)
    implements OperationProjection {
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
