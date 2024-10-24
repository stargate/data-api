package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnType;
import java.io.IOException;

/**
 * Custom serializer to encode the column type to the JSON payload This is required because
 * composite and custom column types may need additional properties to be serialized
 */
public class ColumnDefinitionSerializer extends JsonSerializer<ColumnType> {

  @Override
  public void serialize(
      ColumnType columnType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("type", columnType.getApiName());

    if (columnType instanceof ComplexColumnType.ColumnMapType mt) {
      jsonGenerator.writeStringField("keyType", mt.keyType().getApiName());
      jsonGenerator.writeStringField("valueType", mt.valueType().getApiName());

    } else if (columnType instanceof ComplexColumnType.ColumnListType lt) {
      jsonGenerator.writeStringField("valueType", lt.valueType().getApiName());

    } else if (columnType instanceof ComplexColumnType.ColumnSetType st) {
      jsonGenerator.writeStringField("valueType", st.valueType().getApiName());

    } else if (columnType instanceof ComplexColumnType.ColumnVectorType vt) {
      jsonGenerator.writeNumberField("dimension", vt.getDimensions());
      if (vt.getVectorConfig() != null)
        jsonGenerator.writeObjectField("service", vt.getVectorConfig());

    } else if (columnType instanceof ComplexColumnType.UnsupportedType ut) {
      jsonGenerator.writeObjectField(
          "apiSupport", new ApiSupport(false, false, false, ut.cqlFormat()));
    }
    jsonGenerator.writeEndObject();
  }

  public record ApiSupport(
      boolean createTable, boolean insert, boolean read, String cqlDefinition) {}
}
