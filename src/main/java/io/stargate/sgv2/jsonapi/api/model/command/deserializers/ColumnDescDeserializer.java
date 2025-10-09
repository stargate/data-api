package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.io.IOException;
import java.util.Map;

/**
 * Custom deserializer to decode the {@link ColumnDesc} from the JSON payload
 *
 * <p><b>NOTE:</b> This class exposes static functions so the deserialize code can be reused from
 * other places when needed.
 *
 * <p>The Column Desc can be either a short-form primitive type, which is a text node, or a
 * long-form which is a JSON object with at least a `type` field that is a String.
 *
 * <p>Examples below, each of JSON values on the RHS of the column name is the ColumnDesc that this
 * deserializer can handle.
 *
 * <pre>
 *   {
 *     "columns": {
 *         "short_form_desc": "text",
 *         "long_form_desc": {
 *             "type": "text"
 *         },
 *         "collections_only_long_form": {
 *             "type": "map",
 *             "keyType": "text",
 *             "valueType": "text"
 *         },
 *         "udt_scalar": {
 *             "type": "userDefined",
 *             "udtName": "my_udt"
 *         },
 *         "udt_set": {
 *             "type": "set",
 *             "valueType": {
 *                 "type": "userDefined",
 *                 "udtName": "test_udt"
 *             }
 *         }
 *     }
 * }
 * </pre>
 */
public class ColumnDescDeserializer extends JsonDeserializer<ColumnDesc> {

  private static final String ERR_PREFIX = "The Long Form type definition";
  private static final String ERR_OBJECT_WITH_TYPE =
      ERR_PREFIX
          + " must be a JSON Object with at least a `%s` field that is a String"
              .formatted(TableDescConstants.ColumnDesc.TYPE);

  /**
   * Default constructor for deserializing column definitions for tables, uses {@link
   * io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint#TABLE_COLUMN} .
   *
   * <p>Needed because used as {@link JsonDeserialize} annotation.
   */
  public ColumnDescDeserializer() {}

  /** See {@link #(JsonNode, JsonParser, SchemaDescSource)} */
  @Override
  public ColumnDesc deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

    return deserialize(
        deserializationContext.readTree(jsonParser),
        jsonParser,
        SchemaDescSource.USER_SCHEMA_USAGE);
  }

  /**
   * Re-usable method to deserialize a {@link ColumnDesc} from a {@link JsonNode}
   *
   * @param descNode The JSON node representing the column description, see class comments.
   * @param jsonParser Nullable {@link JsonParser} used to deserialize the node, is used when
   *     creating {@link JsonMappingException}.
   * @param schemaDescSource The {@link SchemaDescSource} to use for the column description.
   * @return The deserialized {@link ColumnDesc} object.
   * @throws JsonProcessingException If there is an error during deserialization, such as a missing
   *     or invalid type.
   * @throws SchemaException if the type name is unknown or not supported by the rule.
   */
  public static ColumnDesc deserialize(
      JsonNode descNode, JsonParser jsonParser, SchemaDescSource schemaDescSource)
      throws JsonProcessingException {

    // throws if type is not defined correctly or not a known type
    var typeNameDesc = getTypeName(descNode, jsonParser);

    // check if this is primitive type, no matter if it is short or long form
    var maybePrimitiveType = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(typeNameDesc.typeName);

    if (maybePrimitiveType.isPresent()) {
      // does not matter if it is short or long form, we can return the primitive type
      return maybePrimitiveType.get();
    }

    // we know it was not a primitive type, but do not throw an error,
    // we want the type binding rules ot run to detect the error.

    // if the nodes are missing, the jackson MissingNode will be returned and has "" and 0 for
    // defaults. but to get a decent error message get the dimension as a string
    // arguable that a non integer is a JSON mapping error, but we will handle it as an unsupported
    // dimension value

    return switch (typeNameDesc.typeName) {
      case LIST -> ListColumnDesc.FROM_JSON_FACTORY.create(schemaDescSource, jsonParser, descNode);
      case SET -> SetColumnDesc.FROM_JSON_FACTORY.create(schemaDescSource, jsonParser, descNode);
      case MAP -> MapColumnDesc.FROM_JSON_FACTORY.create(schemaDescSource, jsonParser, descNode);
      case VECTOR -> {
        // call to readTreeAsValue will throw JacksonException, this should be if the databinding is
        // not correct, e.g. if there is a missing field, or the field is not the correct type
        // ok to let this out
        yield VectorColumnDesc.FROM_JSON_FACTORY.create(schemaDescSource, jsonParser, descNode);
      }
      case UDT -> // The rule tells us if the UDT is frozen or not, see the enum
          UdtRefColumnDesc.FROM_JSON_FACTORY.create(schemaDescSource, jsonParser, descNode);
      default ->
          // sanity check, we should have covered all the API types above
          throw new IllegalStateException(
              "ColumnDescDeserializer - unsupported known APiTypeName: "
                  + typeNameDesc.typeName.apiName());
    };
  }

  @Override
  public ColumnDesc getNullValue(DeserializationContext ctxt) throws JsonMappingException {
    throw new JsonMappingException(ctxt.getParser(), ERR_OBJECT_WITH_TYPE + " (value is null)");
  }

  /**
   * Gets the api type name from the ColumnDesc node, from either the long form or the short form.
   *
   * @param descNode The desc node as defined in class comments.
   * @param jsonParser Nullable {@link JsonParser} used to deserialize the node, is used with the
   *     {@link JsonMappingException} to provide context.
   * @return the {@link ApiTypeName} of the column description.
   * @throws JsonMappingException the node is not a text node or an object with the type field.
   * @throws {@link SchemaException.Code#UNKNOWN_DATA_TYPE} if the type name is present, but not a
   *     know {@link ApiTypeName}
   */
  public static TypeNameDesc getTypeName(JsonNode descNode, JsonParser jsonParser)
      throws JsonMappingException {

    String rawTypeName;
    final boolean shortFormDesc = descNode.isTextual();
    if (shortFormDesc) {
      // in short form, e.g. {"userName": "text"}
      rawTypeName = descNode.textValue();
    } else {
      // in long form, e.g.  {"userName": {"type": "text"}}

      // Must be an object with type field
      if (!descNode.isObject()) {
        throw new JsonMappingException(
            jsonParser,
            ERR_OBJECT_WITH_TYPE
                + " (node is not Object but "
                + JsonUtil.nodeTypeAsString(descNode)
                + ")");
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
                + " (`%s` field is not Text but %s)"
                    .formatted(
                        TableDescConstants.ColumnDesc.TYPE, JsonUtil.nodeTypeAsString(typeNode)));
      }
      rawTypeName = typeNode.asText();
    }

    var maybeTypeName = ApiTypeName.fromApiName(rawTypeName);
    if (maybeTypeName.isEmpty()) {
      if (shortFormDesc) {
        // short form can only be used to describe primitive types so we can throw an error
        // e.g. this not allowed  {"userName": "map"}
        throw SchemaException.Code.UNKNOWN_PRIMITIVE_DATA_TYPE.get(
            Map.of(
                "supportedTypes",
                errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                "unsupportedType",
                rawTypeName));
      } else {
        // using long form it could be a primitive or a non-primitive type
        throw SchemaException.Code.UNKNOWN_DATA_TYPE.get(
            Map.of(
                "supportedTypes",
                errFmtJoin(ApiTypeName.all(), ApiTypeName::apiName),
                "unsupportedType",
                rawTypeName));
      }
    }

    return new TypeNameDesc(maybeTypeName.get(), shortFormDesc);
  }

  /**
   * How the {@link ApiTypeName} was represented in the JSON payload.
   *
   * @param typeName the {@link ApiTypeName} from the column description.
   * @param shortFormDesc True if the type name was represented as a short form text node, False for
   *     long form
   */
  public record TypeNameDesc(ApiTypeName typeName, boolean shortFormDesc) {}
  ;
}
