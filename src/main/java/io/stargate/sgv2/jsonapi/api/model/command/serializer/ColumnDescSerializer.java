package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.UnsupportedColumnDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;

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
    jsonGenerator.writeStringField(TableDescConstants.ColumnDesc.TYPE, columnDesc.getApiName());

    if (columnDesc instanceof ComplexColumnDesc.MapColumnDesc mt) {
      jsonGenerator.writeStringField(TableDescConstants.ColumnDesc.KEY_TYPE, mt.keyType().getApiName());
      jsonGenerator.writeStringField(TableDescConstants.ColumnDesc.VALUE_TYPE, mt.valueType().getApiName());

    } else if (columnDesc instanceof ComplexColumnDesc.ListColumnDesc lt) {
      jsonGenerator.writeStringField(TableDescConstants.ColumnDesc.VALUE_TYPE, lt.valueType().getApiName());

    } else if (columnDesc instanceof ComplexColumnDesc.SetColumnDesc st) {
      jsonGenerator.writeStringField(TableDescConstants.ColumnDesc.VALUE_TYPE, st.valueType().getApiName());

    } else if (columnDesc instanceof ComplexColumnDesc.VectorColumnDesc vt) {
      jsonGenerator.writeNumberField(TableDescConstants.ColumnDesc.DIMENSION, vt.getDimensions());
      if (vt.getVectorConfig() != null)
        jsonGenerator.writeObjectField(TableDescConstants.ColumnDesc.SERVICE, vt.getVectorConfig());

    } else if (columnDesc instanceof UnsupportedColumnDesc ut) {
      jsonGenerator.writeObjectField(TableDescConstants.ColumnDesc.API_SUPPORT, ut.apiSupport());
    }
    jsonGenerator.writeEndObject();
  }


}
