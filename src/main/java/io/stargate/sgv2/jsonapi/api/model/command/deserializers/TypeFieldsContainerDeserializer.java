package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Custom deserializer for container of UDT fields. See {@link TypeDefinitionDesc} for usage.
 * Reusing {@link ColumnDescDeserializer} to handle the deserialization of ColumnDesc.
 */
public class TypeFieldsContainerDeserializer extends JsonDeserializer<ColumnsDescContainer> {

  public TypeFieldsContainerDeserializer() {}

  @Override
  public ColumnsDescContainer deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

    JsonNode node = deserializationContext.readTree(jsonParser);
    ColumnsDescContainer container = new ColumnsDescContainer();

    for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> entry = it.next();
      String fieldName = entry.getKey();
      JsonNode fieldNode = entry.getValue();

      // Deserialize each ColumnDesc
      ColumnDescDeserializer deserializer = new ColumnDescDeserializer(true);
      ColumnDesc columnDesc =
          deserializer.deserialize(fieldNode.traverse(), deserializationContext);
      //      ColumnDesc columnDesc = deserializationContext.readTreeAsValue(fieldNode,
      // ColumnDesc.class);
      container.put(fieldName, columnDesc);
    }

    return container;
  }
}
