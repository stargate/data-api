package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
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

    switch (columnDesc) {
      case MapColumnDesc mt -> writeMapSetListDesc(jsonGenerator, mt);
      case ListColumnDesc lt -> writeMapSetListDesc(jsonGenerator, lt);
      case SetColumnDesc st -> writeMapSetListDesc(jsonGenerator, st);
      case VectorColumnDesc vt -> {
        jsonGenerator.writeNumberField(TableDescConstants.ColumnDesc.DIMENSION, vt.getDimension());
        if (vt.getVectorizeConfig() != null)
          jsonGenerator.writeObjectField(
              TableDescConstants.ColumnDesc.SERVICE, vt.getVectorizeConfig());
      }
      case UDTColumnDesc udt -> writeFullUdtDesc(jsonGenerator, udt);
      default -> {
        // nothing extra to do , an unsupported type will be picked up below checking aipSupport()
      }
    }

    if (columnDesc.apiSupport().isAnyUnsupported()) {
      jsonGenerator.writeObjectField(
          TableDescConstants.ColumnDesc.API_SUPPORT, columnDesc.apiSupport());
    }

    jsonGenerator.writeEndObject();
  }

  /**
   * Write the map/set/list column description to the JSON generator.
   *
   * <p>Note, if the key/value type is a UDT, it will write the full UDT description. Otherwise, it
   * will just write the type name.
   */
  private void writeMapSetListDesc(JsonGenerator jsonGenerator, ColumnDesc mapSetListColumnDesc)
      throws IOException {

    switch (mapSetListColumnDesc) {
      case MapColumnDesc mapColumnDesc -> {
        if (mapColumnDesc.keyType() instanceof UDTColumnDesc udtKey) {
          jsonGenerator.writeObjectFieldStart(TableDescConstants.ColumnDesc.KEY_TYPE);
          writeFullUdtDesc(jsonGenerator, udtKey);
          jsonGenerator.writeEndObject();
        } else {
          jsonGenerator.writeStringField(
              TableDescConstants.ColumnDesc.KEY_TYPE, mapColumnDesc.keyType().getApiName());
        }
        if (mapColumnDesc.valueType() instanceof UDTColumnDesc udtValue) {
          jsonGenerator.writeObjectFieldStart(TableDescConstants.ColumnDesc.VALUE_TYPE);
          writeFullUdtDesc(jsonGenerator, udtValue);
          jsonGenerator.writeEndObject();
        } else {
          jsonGenerator.writeStringField(
              TableDescConstants.ColumnDesc.VALUE_TYPE, mapColumnDesc.valueType().getApiName());
        }
      }
      case SetColumnDesc setColumnDesc -> {
        if (setColumnDesc.valueType() instanceof UDTColumnDesc udtValue) {
          jsonGenerator.writeObjectFieldStart(TableDescConstants.ColumnDesc.VALUE_TYPE);
          writeFullUdtDesc(jsonGenerator, udtValue);
          jsonGenerator.writeEndObject();
        } else {
          jsonGenerator.writeStringField(
              TableDescConstants.ColumnDesc.VALUE_TYPE, setColumnDesc.valueType().getApiName());
        }
      }
      case ListColumnDesc listColumnDesc -> {
        if (listColumnDesc.valueType() instanceof UDTColumnDesc udtValue) {
          jsonGenerator.writeObjectFieldStart(TableDescConstants.ColumnDesc.VALUE_TYPE);
          writeFullUdtDesc(jsonGenerator, udtValue);
          jsonGenerator.writeEndObject();
        } else {
          jsonGenerator.writeStringField(
              TableDescConstants.ColumnDesc.VALUE_TYPE, listColumnDesc.valueType().getApiName());
        }
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported column type for map/set/list: " + mapSetListColumnDesc.getApiName());
    }
  }

  private void writeFullUdtDesc(JsonGenerator jsonGenerator, UDTColumnDesc udtColumnDesc)
      throws IOException {
    jsonGenerator.writeStringField(
        TableDescConstants.ColumnDesc.UDT_NAME, udtColumnDesc.udtName().asInternal());
    jsonGenerator.writeObjectFieldStart(TableDescConstants.ColumnDesc.DEFINITION);
    jsonGenerator.writeObjectField(
        TableDescConstants.ColumnDesc.FIELDS, udtColumnDesc.fieldsDesc());
    jsonGenerator.writeEndObject();
  }
}
