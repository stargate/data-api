package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.io.IOException;

/**
 * Custom deserializer to decode the column type from the JSON payload This is required because
 * composite and custom column types may need additional properties to be deserialized
 */
public class ColumnDefinitionDeserializer extends StdDeserializer<ColumnType> {

  public ColumnDefinitionDeserializer() {
    super(ColumnType.class);
  }

  @Override
  public ColumnType deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode definition = deserializationContext.readTree(jsonParser);
    if (definition.isTextual()) {
      return ColumnType.fromString(definition.asText());
    }
    if (!definition.has("type")) {
      throw ErrorCodeV1.TABLE_COLUMN_TYPE_NOT_PROVIDED.toApiException();
    }
    return ColumnType.fromString(definition.path("type").asText());
  }
}
