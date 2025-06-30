package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Custom deserializer to decode the column type from the JSON payload This is required because
 * composite and custom column types may need additional properties to be deserialized
 */
public class ColumnDescDeserializer extends JsonDeserializer<ColumnDesc> {

  private static final String ERR_PREFIX = "The Long Form type definition";
  public static final String ERR_OBJECT_WITH_TYPE =
      ERR_PREFIX
          + " must be a JSON Object with at least a `%s` field that is a String"
              .formatted(TableDescConstants.ColumnDesc.TYPE);

  // This is the flag to indicate whether we are deserializing UDT fields or not.
  // By default, it is set to false, meaning we are serializing column definitions for commands like
  // CreateTable, AlterTable.
  private final boolean deserializeUDTFields;

  public ColumnDescDeserializer() {
    this.deserializeUDTFields = false;
  }

  public ColumnDescDeserializer(boolean deserializeUDTFields) {
    this.deserializeUDTFields = deserializeUDTFields;
  }

  @Override
  public ColumnDesc deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

    JsonNode descNode = deserializationContext.readTree(jsonParser);

    // Check if we are deserializing UDT fields and if so, check for unsupported UDT fields.
    if (deserializeUDTFields) {
      return deserializeForUdtField(descNode, jsonParser);
    }

    if (descNode.isTextual()) {
      // must be a primitive type, that is all that is allowed to only have a type
      return PrimitiveColumnDesc.FROM_JSON_FACTORY
          .create(descNode.asText())
          .orElseThrow(
              () ->
                  SchemaException.Code.UNKNOWN_PRIMITIVE_DATA_TYPE.get(
                      Map.of(
                          "supportedTypes", errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                          "unsupportedType", descNode.asText())));
    }

    // Check we are using long form
    if (!descNode.isObject()) {
      throw new JsonMappingException(jsonParser, ERR_OBJECT_WITH_TYPE + " (node is not object)");
    }
    var typeNode = descNode.path(TableDescConstants.ColumnDesc.TYPE);
    if (typeNode.isMissingNode()) {
      throw new JsonMappingException(
          jsonParser,
          ERR_OBJECT_WITH_TYPE
              + " (`%s` field is missing)".formatted(TableDescConstants.ColumnDesc.TYPE));
    }
    if (!typeNode.isTextual()) {
      throw new JsonMappingException(
          jsonParser,
          ERR_OBJECT_WITH_TYPE
              + " (`%s` field is not String)".formatted(TableDescConstants.ColumnDesc.TYPE));
    }

    // Using long form, things are different. The type could be any type, not just a primitive
    var typeName =
        ApiTypeName.fromApiName(typeNode.asText())
            .orElseThrow(
                () ->
                    SchemaException.Code.UNKNOWN_DATA_TYPE.get(
                        Map.of(
                            "supportedTypes", errFmtJoin(ApiTypeName.all(), ApiTypeName::apiName),
                            "unsupportedType", typeNode.asText())));

    // ok, this could still be a primitive type, so let's check that first
    var longFormPrimitive = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(typeName.apiName());
    if (longFormPrimitive.isPresent()) {
      return longFormPrimitive.get();
    }

    // if the nodes are missing, the jackson MissingNode will be returned and has "" and 0 for
    // defaults.
    // but to get a decent error message get the dimension as a string
    // arguable that a non integer is a JSON mapping error, but we will handle it as an unsupported
    // dimension value
    var keyTypeName = descNode.path(TableDescConstants.ColumnDesc.KEY_TYPE);
    var valueTypeName = descNode.path(TableDescConstants.ColumnDesc.VALUE_TYPE);
    var dimensionString = descNode.path(TableDescConstants.ColumnDesc.DIMENSION).asText();

    return switch (typeName) {
      case LIST -> ListColumnDesc.FROM_JSON_FACTORY.create(jsonParser, valueTypeName);
      case SET -> SetColumnDesc.FROM_JSON_FACTORY.create(jsonParser, valueTypeName);
      case MAP -> MapColumnDesc.FROM_JSON_FACTORY.create(jsonParser, keyTypeName, valueTypeName);
      case VECTOR -> {
        // call to readTreeAsValue will throw JacksonException, this should be if the databinding is
        // not correct, e.g. if there is a missing field, or the field is not the correct type
        // ok to let this out
        var serviceNode = descNode.path(TableDescConstants.ColumnDesc.SERVICE);
        VectorizeConfig vectorConfig =
            serviceNode.isMissingNode()
                ? null
                : deserializationContext.readTreeAsValue(serviceNode, VectorizeConfig.class);
        yield VectorColumnDesc.FROM_JSON_FACTORY.create(dimensionString, vectorConfig);
      }
      case UDT -> {
        var udtName = descNode.path(TableDescConstants.ColumnDesc.UDT_NAME).asText();
        // As of June 26th 2025, API only supports non-frozen UDT column to create.
        yield UDTColumnDesc.FROM_JSON_FACTORY.create(udtName, false);
      }
        // should not get here, because we checked the API type name above
      default ->
          throw new IllegalStateException("No match for known typeName: " + typeName.apiName());
    };
  }

  @Override
  public ColumnDesc getNullValue(DeserializationContext ctxt) throws JsonMappingException {
    throw new JsonMappingException(ctxt.getParser(), ERR_OBJECT_WITH_TYPE + " (value is null)");
  }

  /**
   * Rules for current supported UDT field definition from API user request. As of June 16th 2025,
   * UDT as field, map/set/list as field are not supported.
   *
   * <p>Note: You will see there is code redundancy in this method compared to {@link
   * ColumnDescDeserializer::deserialize(JsonParser, DeserializationContext)}. This is to keep the
   * deserialization logic for UDT fields separate from the general table column.
   */
  private ColumnDesc deserializeForUdtField(JsonNode descNode, JsonParser jsonParser)
      throws JsonMappingException {

    if (descNode.isTextual()) {
      // short-form must be a primitive type, that is all that is allowed to only have a type
      return PrimitiveColumnDesc.FROM_JSON_FACTORY
          .create(descNode.asText())
          .orElseThrow(
              () ->
                  SchemaException.Code.UNKNOWN_PRIMITIVE_DATA_TYPE.get(
                      Map.of(
                          "supportedTypes", errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                          "unsupportedType", descNode.asText())));
    }

    // Check we are using long form
    if (!descNode.isObject()) {
      throw new JsonMappingException(jsonParser, ERR_OBJECT_WITH_TYPE + " (node is not object)");
    }
    var typeNode = descNode.path(TableDescConstants.ColumnDesc.TYPE);
    if (typeNode.isMissingNode()) {
      throw new JsonMappingException(
          jsonParser,
          ERR_OBJECT_WITH_TYPE
              + " (`%s` field is missing)".formatted(TableDescConstants.ColumnDesc.TYPE));
    }
    if (!typeNode.isTextual()) {
      throw new JsonMappingException(
          jsonParser,
          ERR_OBJECT_WITH_TYPE
              + " (`%s` field is not String)".formatted(TableDescConstants.ColumnDesc.TYPE));
    }

    // long-form to get the apiTypeName
    final Optional<ApiTypeName> apiTypeName = ApiTypeName.fromApiName(typeNode.asText());
    if (apiTypeName.isPresent()) {
      var longFormPrimitive =
          PrimitiveColumnDesc.FROM_JSON_FACTORY.create(apiTypeName.get().apiName());
      if (longFormPrimitive.isPresent()) {
        return longFormPrimitive.get();
      }
    }

    // UDT as field or map/set/list as field both requires long form of definition.
    throw SchemaException.Code.UNSUPPORTED_TYPE_FIELD.get(
        "unsupportedType", typeNode.asText(),
        "supportedTypes", errFmtJoin(ApiTypeName.all(), ApiTypeName::apiName));
  }
}
