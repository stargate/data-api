package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

/**
 * Serializer for a container of {@link ApiColumnDef} objects when returning the schema in a
 * response.
 *
 * <p>Example:
 *
 * <pre>
 * {
 *     "partition": {
 *         "type": "text"
 *     },
 *     "key": {
 *         "type": "double"
 *     }
 * }
 * </pre>
 *
 * Formatting of the tpes handed to the {@link PrimitiveApiDataTypeDef} and its serializer.
 */
public class ApiColumnDefContainerSerializer extends JsonSerializer<ApiColumnDefContainer> {

  @Override
  public void serialize(
      ApiColumnDefContainer container, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    for (var apiColumnDef : container.values()) {
      gen.writeFieldName(cqlIdentifierToJsonKey(apiColumnDef.name()));
      gen.writeObject(apiColumnDef.type());
    }
    gen.writeEndObject();
  }
}
