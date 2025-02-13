package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import java.io.IOException;

/**
 * Custom serializer to encode the RegularIndexColumn to the JSON payload.
 *
 * <p>This is required because there may be additional properties to be serialized.
 *
 * <p>E.G. Serialize result:
 *
 * <ul>
 *   <li>list column that is index on values: <code>{"column": "listColumn", "indexOn": "values"}
 *       </code>
 *   <li>set column that is index on values: <code>{"column": "setColumn", "indexOn": "values"}
 *       </code>
 *   <li>map column that is index on keys: <code>{"column": "mapColumn", "indexOn": "keys"}</code>
 *   <li>map column that is index on values: <code>{"column": "mapColumn", "indexOn": "values"}
 *       </code>
 *   <li>map column that is index on entries: <code>{"column": "mapColumn"}</code>
 *   <li>primitive column with index: <code>{"column": "age"}</code>
 * </ul>
 */
public class RegularIndexColumnSerializer
    extends JsonSerializer<RegularIndexDefinitionDesc.RegularIndexColumn> {

  @Override
  public void serialize(
      RegularIndexDefinitionDesc.RegularIndexColumn regularIndexColumn,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider)
      throws IOException {
    if (regularIndexColumn.mapComponent() != null) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField(
          regularIndexColumn.columnName(), regularIndexColumn.mapComponent().getApiName());
      jsonGenerator.writeEndObject();
    } else {
      jsonGenerator.writeString(regularIndexColumn.columnName());
    }
  }
}
