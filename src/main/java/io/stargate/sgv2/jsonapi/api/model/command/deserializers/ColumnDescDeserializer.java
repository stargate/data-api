package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.io.IOException;

/**
 * Custom deserializer to decode the column type from the JSON payload This is required because
 * composite and custom column types may need additional properties to be deserialized
 */
public class ColumnDescDeserializer extends StdDeserializer<ColumnDesc> {

  public ColumnDescDeserializer() {
    super(ColumnDesc.class);
  }

  @Override
  public ColumnDesc deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode definition = deserializationContext.readTree(jsonParser);
    if (definition.isTextual()) {
      return ColumnDesc.fromJsonString(definition.asText(), null, null, -1, null);
    }
    if (definition.isObject() && definition.has("type")) {
      String type = definition.path("type").asText();
      String keyType = null;
      String valueType = null;
      int dimension = -1;
      VectorizeConfig vectorConfig = null;
      if (definition.has("keyType")) {
        keyType = definition.path("keyType").asText();
      }
      if (definition.has("valueType")) {
        valueType = definition.path("valueType").asText();
      }
      if (definition.has("dimension")) {
        dimension = definition.path("dimension").asInt();
      }
      if (definition.has("service")) {
        JsonNode service = definition.path("service");
        try {
          vectorConfig = deserializationContext.readTreeAsValue(service, VectorizeConfig.class);
        } catch (JacksonException je) {
          throw SchemaException.Code.VECTOR_TYPE_INVALID_DEFINITION.get();
        }
      }
      return ColumnDesc.fromJsonString(type, keyType, valueType, dimension, vectorConfig);
    }
    throw SchemaException.Code.COLUMN_TYPE_INCORRECT.get();
  }

  @Override
  public ColumnDesc getNullValue(DeserializationContext ctxt) {
    throw SchemaException.Code.COLUMN_TYPE_INCORRECT.get();
  }
}
