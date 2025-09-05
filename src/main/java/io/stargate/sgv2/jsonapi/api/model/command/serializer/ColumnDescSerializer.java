package io.stargate.sgv2.jsonapi.api.model.command.serializer;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import java.io.IOException;

/**
 * Custom serializer to encode the {@link ColumnDesc} to JSON payload.
 *
 * <p>See comments on {@link ColumnDescDeserializer} for the format this will create.
 *
 * <p>This will always write the long form of the column description as below:
 *
 * <pre>
 *   {
 *      "userName": {
 *        "type": "text"
 *      },
 *      "addresses": {
 *        "type": "map",
 *        "keyType": {
 *          "type": "text"
 *        }
 *      }
 *      "valueType": {
 *        "type": "userDefined",
 *        "udtName": "address"
 *      },
 *      "favColours" : {
 *        "type": "list",
 *        "valueType": {
 *          "type": "text"
 *        }
 *      }
 *      // NOTE: for read response inline schema we include the full UDT definition from UdtColumnDesc
 *      "address": {
 *        "type": "userDefined",
 *        "udtName": "address",
 *        "definition": {
 * 			    "fields": {
 *            "country": "text",
 *        	  "city": "text"
 *          }
 *        }
 *      }
 *   }
 * </pre>
 */
public class ColumnDescSerializer extends JsonSerializer<ColumnDesc> {

  @Override
  public void serialize(
      ColumnDesc columnDesc, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {

    jsonGenerator.writeStartObject();
    writeDesc(jsonGenerator, columnDesc);
    jsonGenerator.writeEndObject();
  }

  /**
   * Write for a generic {@link ColumnDesc} to the JSON generator.
   *
   * <p>The JSON object we are writing to should be started
   */
  private void writeDesc(JsonGenerator json, ColumnDesc columnDesc) throws IOException {

    // they always have a type field
    json.writeStringField(TableDescConstants.ColumnDesc.TYPE, columnDesc.getApiName());

    switch (columnDesc) {
      case MapColumnDesc mt -> {
        newColumnDescObject(json, TableDescConstants.ColumnDesc.KEY_TYPE, mt.keyType());
        newColumnDescObject(json, TableDescConstants.ColumnDesc.VALUE_TYPE, mt.valueType());
      }
      case ListColumnDesc lt -> {
        newColumnDescObject(json, TableDescConstants.ColumnDesc.VALUE_TYPE, lt.valueType());
      }
      case SetColumnDesc st -> {
        newColumnDescObject(json, TableDescConstants.ColumnDesc.VALUE_TYPE, st.valueType());
      }
      case VectorColumnDesc vt -> {
        json.writeNumberField(TableDescConstants.ColumnDesc.DIMENSION, vt.getDimension());
        if (vt.getVectorizeConfig() != null)
          json.writeObjectField(TableDescConstants.ColumnDesc.SERVICE, vt.getVectorizeConfig());
      }

      case UdtColumnDesc udtCol -> {
        // Full UDT definition, not just the name, must go before UdtRefColumnDesc superclass

        json.writeStringField(
            TableDescConstants.ColumnDesc.UDT_NAME, cqlIdentifierToJsonKey(udtCol.udtName()));

        // begin "definition" object
        json.writeObjectFieldStart(TableDescConstants.ColumnDesc.DEFINITION);

        // begin "definition -> fields" object
        json.writeObjectFieldStart(TableDescConstants.ColumnDesc.FIELDS);

        for (var entry : udtCol.allFields().entrySet()) {
          // begin "definition -> fields -> <field_name>" object
          json.writeObjectFieldStart(entry.getKey());

          writeDesc(json, entry.getValue());

          // end "definition -> fields -> <field_name>" object
          json.writeEndObject();
        }
        // end "definition -> fields" object
        json.writeEndObject();

        // end "definition" object
        json.writeEndObject();
      }

      case UdtRefColumnDesc udt -> {
        json.writeStringField(
            TableDescConstants.ColumnDesc.UDT_NAME, cqlIdentifierToJsonKey(udt.udtName()));
      }
      default -> {
        // nothing extra to do , an unsupported type will be picked up below checking aipSupport()
      }
    }

    if (columnDesc.apiSupport().isAnyUnsupported()) {
      json.writeObjectField(TableDescConstants.ColumnDesc.API_SUPPORT, columnDesc.apiSupport());
    }
  }

  /**
   * Write a new object field for a column description that is not primitive. July 16th, 2025, write
   * string field for primitive types, this is to not break client, value/key types of map/list/set
   * are still return with short-form.
   */
  private void newColumnDescObject(JsonGenerator json, String fieldName, ColumnDesc columnDesc)
      throws IOException {

    // July 16th, 2025. To not break client, value/key types of map/list/set are still return with
    // short-form
    if (columnDesc.typeName().isPrimitive()) {
      json.writeStringField(fieldName, columnDesc.getApiName());
      return;
    }

    json.writeObjectFieldStart(fieldName);
    writeDesc(json, columnDesc);
    json.writeEndObject();
  }
}
