package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import java.io.IOException;

/**
 * Custom serializer to encode the RegularIndexColumn to the JSON payload. This is required because
 * there may be additional properties to be serialized. <br>
 * E.G. Serialize result
 *
 * <pre>
 * list column that is index on values: {"column": "listColumn", "indexOn": "values"}
 * set column that is index on values: {"column": "setColumn", "indexOn": "values"}
 * map column that is index on keys: {"column": "mapColumn", "indexOn": "keys"}
 * map column that is index on values: {"column": "mapColumn", "indexOn": "values"}
 * map column that is index on entries: {"column": "mapColumn"}
 * primitive column with index: {"column": "age"}
 * </pre>
 */
public class RegularIndexColumnSerializer
    extends JsonSerializer<RegularIndexDefinitionDesc.RegularIndexColumn> {

  @Override
  public void serialize(
      RegularIndexDefinitionDesc.RegularIndexColumn regularIndexColumn,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider)
      throws IOException {
    if (regularIndexColumn.indexOnMapComponent() != null) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField(
          regularIndexColumn.columnName(), regularIndexColumn.indexOnMapComponent().getValue());
      jsonGenerator.writeEndObject();
    } else {
      jsonGenerator.writeString(regularIndexColumn.columnName());
    }
  }
}
