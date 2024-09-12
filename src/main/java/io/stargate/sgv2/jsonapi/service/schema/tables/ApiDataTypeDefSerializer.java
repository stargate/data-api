package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import java.io.IOException;

/**
 * Serialise {@link ApiDataTypeDef} objects for use in responses.
 *
 * <p>Example, this generates the JSON for type sub-object
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
 */
public class ApiDataTypeDefSerializer extends JsonSerializer<ApiDataTypeDef> {

  @Override
  public void serialize(
      ApiDataTypeDef dataTypeDef, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {

    gen.writeStartObject();
    gen.writeStringField(
        SchemaConstants.DataTypeFields.TYPE, dataTypeDef.getApiType().getApiName());
    gen.writeEndObject();
  }
}
