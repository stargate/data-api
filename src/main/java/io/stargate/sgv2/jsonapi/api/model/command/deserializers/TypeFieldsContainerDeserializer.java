package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import java.io.IOException;
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

    var container = new ColumnsDescContainer();
    var fieldsIter = deserializationContext.readTree(jsonParser).fields();

    // cannot use forEach because want IOException to propagate
    while (fieldsIter.hasNext()) {

      // we aare deserializing from the user, so using USER_SCHEMA_OBJECT
      Map.Entry<String, JsonNode> entry = fieldsIter.next();
      container.put(
          entry.getKey(),
          ColumnDescDeserializer.deserialize(
              entry.getValue(), jsonParser, SchemaDescSource.USER_SCHEMA_OBJECT));
    }
    return container;
  }
}
