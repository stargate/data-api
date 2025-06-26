package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer.ERR_OBJECT_WITH_TYPE;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Interface for complex column types like collections */
public abstract class ComplexColumnDesc implements ColumnDesc {

  private final ApiTypeName apiTypeName;
  private final ApiSupportDesc apiSupportDesc;

  protected ComplexColumnDesc(ApiTypeName apiTypeName, ApiSupportDesc apiSupportDesc) {
    this.apiTypeName = Objects.requireNonNull(apiTypeName, "apiTypeName must not be null");
    this.apiSupportDesc = Objects.requireNonNull(apiSupportDesc, "apiSupportDesc must not be null");
    ;
  }

  @Override
  public ApiTypeName typeName() {
    return apiTypeName;
  }

  @Override
  public ApiSupportDesc apiSupport() {
    return apiSupportDesc;
  }

  /**
   * Method to determine the element type for a Map, Set, or List column based on the provided
   * jsonNode. Param elementTypeNode can be either:
   *
   * <ul>
   *   <li>short-form text node: primitive type
   *   <li>long-form object node: primitive type or UDT type
   * </ul>
   *
   * Note, we do not support map key as UDT. If no valid columnDesc can be mapped, method will
   * return empty optional and let the caller handle the error.
   *
   * @param isValueTypeNode false if it is for map key, true if it is for map/set/list value
   */
  public static Optional<ColumnDesc> elementTypeForMapSetListColumn(
      JsonParser jsonParser, JsonNode elementTypeNode, boolean isValueTypeNode)
      throws JsonMappingException {

    Optional<ColumnDesc> elementTypeDesc = Optional.empty();

    // if it's a short-form primitive type, it will be a text node
    if (elementTypeNode.isTextual()) {
      // shortFormPrimitive
      elementTypeDesc = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(elementTypeNode.asText());
    }

    // if it's an object, it can be a primitive type or UDT type
    if (elementTypeNode.isObject()) {
      var type = elementTypeNode.get(TableDescConstants.ColumnDesc.TYPE);
      if (type == null || type.isMissingNode()) {
        throw new JsonMappingException(
            jsonParser,
            ERR_OBJECT_WITH_TYPE
                + " (`%s` field is missing)".formatted(TableDescConstants.ColumnDesc.TYPE));
      }
      if (!type.isTextual()) {
        throw new JsonMappingException(
            jsonParser,
            ERR_OBJECT_WITH_TYPE
                + " (`%s` field is not String)".formatted(TableDescConstants.ColumnDesc.TYPE));
      }
      var typeName =
          ApiTypeName.fromApiName(type.asText())
              .orElseThrow(
                  () ->
                      SchemaException.Code.UNKNOWN_DATA_TYPE.get(
                          Map.of(
                              "supportedTypes", errFmtJoin(ApiTypeName.all(), ApiTypeName::apiName),
                              "unsupportedType", type.asText())));

      var longFormPrimitive = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(typeName.apiName());
      if (longFormPrimitive.isPresent()) {
        elementTypeDesc = longFormPrimitive;
      } else {
        // We don't support UDT for map key type
        if (typeName.equals(ApiTypeName.UDT) && isValueTypeNode) {
          // long-form UDT type, we need to get the UDT name
          var udtName = elementTypeNode.path(TableDescConstants.ColumnDesc.UDT_NAME).asText();
          // As of June 23th 2025, API only supports frozen UDT as map/set/list component to create.
          elementTypeDesc = Optional.of(UDTColumnDesc.FROM_JSON_FACTORY.create(udtName, true));
        }
        // else won't set valueTypeDesc, and fall through to result an empty optional
      }
    }

    return elementTypeDesc;
  }
}
