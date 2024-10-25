package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnDesc;
import java.io.IOException;

/**
 * Custom serializer to encode the column type to the JSON payload This is required because
 * composite and custom column types may need additional properties to be serialized
 */
public class ColumnDescSerializer extends JsonSerializer<ColumnDesc> {

  @Override
  public void serialize(
      ColumnDesc columnDesc, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("type", columnDesc.getApiName());

    if (columnDesc instanceof ComplexColumnDesc.MapColumnDesc mt) {
      jsonGenerator.writeStringField("keyType", mt.keyType().getApiName());
      jsonGenerator.writeStringField("valueType", mt.valueType().getApiName());

    } else if (columnDesc instanceof ComplexColumnDesc.ListColumnDesc lt) {
      jsonGenerator.writeStringField("valueType", lt.valueType().getApiName());

    } else if (columnDesc instanceof ComplexColumnDesc.SetColumnDesc st) {
      jsonGenerator.writeStringField("valueType", st.valueType().getApiName());

    } else if (columnDesc instanceof ComplexColumnDesc.VectorColumnDesc vt) {
      jsonGenerator.writeNumberField("dimension", vt.getDimensions());
      if (vt.getVectorConfig() != null)
        jsonGenerator.writeObjectField("service", vt.getVectorConfig());

    } else if (columnDesc instanceof ComplexColumnDesc.UnsupportedType ut) {
      jsonGenerator.writeObjectField(
          "apiSupport", new ApiSupport(false, false, false, ut.cqlFormat()));
    }
    jsonGenerator.writeEndObject();
  }

  public record ApiSupport(
      boolean createTable, boolean insert, boolean read, String cqlDefinition) {}
}
