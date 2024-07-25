package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.column.definition.ColumnDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.column.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.io.IOException;

/**
 * Custom deserializer to decode the column type from the JSON payload This is required because
 * composite and custom column types may need additional properties to be deserialized
 */
public class ColumnDefinitionDeserializer extends StdDeserializer<ColumnDefinition> {

  public ColumnDefinitionDeserializer() {
    super(ColumnDefinition.class);
  }

  @Override
  public ColumnDefinition deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode definition = deserializationContext.readTree(jsonParser);
    if (definition.has("type")) {
      throw ErrorCode.COLUMN_TYPE_NOT_PROVIDED.toApiException();
    }
    ColumnType type = ColumnType.fromString(definition.path("type").asText());
    return new ColumnDefinition(type);
  }
}
