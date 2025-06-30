package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import java.io.IOException;

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
    ColumnDescDeserializer deserializer = new ColumnDescDeserializer(true);

    node.fields()
        .forEachRemaining(
            entry -> {
              var fieldName = entry.getKey();
              var fieldNode = entry.getValue();

              // Deserialize each ColumnDesc
              var columnDesc =
                  deserializer.deserialize(fieldNode.traverse(), deserializationContext);
              container.put(fieldName, columnDesc);
            });

    return container;
  }
}
