package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.io.IOException;
import java.util.Map;

/**
 * Custom deserializer to decode the column type from the JSON payload This is required because
 * composite and custom column types may need additional properties to be deserialized
 */
public class ColumnDescDeserializer extends StdDeserializer<ColumnDesc> {

  private static final String ERR_PREFIX = "The Long Form type definition";
  private static final String ERR_OBJECT_WITH_TYPE =
      ERR_PREFIX
          + " must be a JSON Object with at least a `%s` field that is a String"
              .formatted(TableDescConstants.ColumnDesc.TYPE);

  public ColumnDescDeserializer() {
    super(ColumnDesc.class);
  }

  @Override
  public ColumnDesc deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {

    JsonNode descNode = deserializationContext.readTree(jsonParser);
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
    var keyTypeName = descNode.path(TableDescConstants.ColumnDesc.KEY_TYPE).asText();
    var valueTypeName = descNode.path(TableDescConstants.ColumnDesc.VALUE_TYPE).asText();
    var dimensionString = descNode.path(TableDescConstants.ColumnDesc.DIMENSION).asText();

    return switch (typeName) {
      case LIST -> ListColumnDesc.FROM_JSON_FACTORY.create(valueTypeName);
      case SET -> SetColumnDesc.FROM_JSON_FACTORY.create(valueTypeName);
      case MAP -> MapColumnDesc.FROM_JSON_FACTORY.create(keyTypeName, valueTypeName);
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
        // should not get here, because we checked the API type name above
      default ->
          throw new IllegalStateException("No match for known typeName: " + typeName.apiName());
    };
  }

  @Override
  public ColumnDesc getNullValue(DeserializationContext ctxt) throws JsonMappingException {
    throw new JsonMappingException(ctxt.getParser(), ERR_OBJECT_WITH_TYPE + " (value is null)");
  }
}
