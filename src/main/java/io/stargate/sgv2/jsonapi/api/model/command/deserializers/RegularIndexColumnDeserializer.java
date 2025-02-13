package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.ApiMapComponent;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiRegularIndex;
import java.io.IOException;
import java.util.Map;

/**
 * Deserializes the RegularIndexColumn from json node of column. <br>
 * Primitive column: {"column": "age"}, indexFunction is null <br>
 * List column: {"column": "listColumn"} <br>
 * Set column: {"column": "setColumn"} <br>
 * Map column:
 *
 * <pre>
 *   - Default to index on map entries: {"column": "mapColumn"}
 *   - Index on map keys: {"column": {"mapColumn" : "$keys"}}
 *   - Index on map values: {"column": {"mapColumn" : "$values"}}
 *   </pre>
 *
 * NOTE, this is just index function from user input, validation and default values are in {@link
 * ApiRegularIndex} since we need the ColumnMetaData
 */
public class RegularIndexColumnDeserializer
    extends StdDeserializer<RegularIndexDefinitionDesc.RegularIndexColumn> {
  protected RegularIndexColumnDeserializer() {
    super(RegularIndexDefinitionDesc.RegularIndexColumn.class);
  }

  @Override
  public RegularIndexDefinitionDesc.RegularIndexColumn deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode columnNode = deserializationContext.readTree(jsonParser);

    if (columnNode.isTextual()) {
      // E.G. {"column": "age"}
      // E.G. {"column": "mapColumn"}, default to entries for map
      return new RegularIndexDefinitionDesc.RegularIndexColumn(columnNode.textValue(), null);
    } else if (columnNode.isObject() && columnNode.size() == 1) {
      Map.Entry<String, JsonNode> entry = columnNode.fields().next();
      if (entry.getValue().isTextual()) {
        // E.G. {"column": {"mapColumn" : "$keys"}}
        // E.G. {"column": {"mapColumn" : "$values"}}
        var apiMapComponent =
            ApiMapComponent.fromApiName(entry.getValue().textValue())
                .orElseThrow(SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN::get);
        return new RegularIndexDefinitionDesc.RegularIndexColumn(entry.getKey(), apiMapComponent);
      }
    }
    // E.G. {"column": {"mapColumn" : 123}}
    throw SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN.get();
  }
}
