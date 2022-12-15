package io.stargate.sgv3.docsapi.api.model.command.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * {@link JsonSerializer} for the {@link CommandResult.Error} that serializes error fields in the
 * same level as the message.
 */
public class ErrorSerializer extends JsonSerializer<CommandResult.Error> {

  @Override
  public void serialize(
      CommandResult.Error value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();

    // always write message
    gen.writePOJOField("message", value.message());

    if (null != value.fields()) {
      // then iterate over fields
      for (Map.Entry<String, Object> entry : value.fields().entrySet()) {
        String fieldName = entry.getKey();
        Object fieldValue = entry.getValue();

        // skip writing the message twice
        if (Objects.equals("message", fieldName)) {
          continue;
        }
        gen.writePOJOField(fieldName, fieldValue);
      }
    }

    gen.writeEndObject();
  }
}
