package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;

public class UDTCodecs {

  /** API UDT value representation is Map. */
  private static final GenericType<Map<Object, Object>> GENERIC_UDT =
      GenericType.mapOf(Object.class, Object.class);

  /**
   * Factory method to build a codec for converting a UDT type to CQL: codec will be given set of
   * possible codecs (since we have one per input JSON type) and will dynamically select the right
   * one based on the actual UDT field values.
   *
   * @param fieldsCodecCandidates Map of field names to a list of possible codecs for that field.
   * @param fieldTypes Map of field names to their expected CQL data types.
   * @param userDefinedType The CQL user-defined type (UDT) that this codec will convert to.
   */
  public static JSONCodec<?, UdtValue> buildToCqlUdtCodec(
      Map<CqlIdentifier, List<JSONCodec<?, ?>>> fieldsCodecCandidates,
      Map<CqlIdentifier, DataType> fieldTypes,
      UserDefinedType userDefinedType) {

    return new JSONCodec<>(
        GENERIC_UDT,
        userDefinedType,
        (cqlType, value) -> toCqlUdt(fieldsCodecCandidates, fieldTypes, userDefinedType, value),
        // This code only for to-cql case, not to-json, so set to null.
        null);
  }

  /**
   * Factory method to build a codec for converting a UDT to Json: codec will be given set of
   * possible codecs (since we have one per input JSON type) and will dynamically select the right
   * one based on the actual UDT field values.
   *
   * @param fieldCodecs Map of field names to their expected JSON codecs.
   * @param userDefinedType The CQL user-defined type (UDT) type.
   */
  public static JSONCodec<?, UdtValue> buildToJsonUdtCodec(
      Map<CqlIdentifier, JSONCodec<?, ?>> fieldCodecs, UserDefinedType userDefinedType) {
    return new JSONCodec<>(
        GENERIC_UDT,
        userDefinedType,
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) -> cqlUdtToJsonNode(fieldCodecs, objectMapper, value));
  }

  public record RawUdtField(CqlIdentifier identifier, DataType cqlType) {}

  public static List<RawUdtField> udtRawFields(UserDefinedType userDefinedType) {

    Objects.requireNonNull(userDefinedType, "userDefinedType must not be null");

    var rawFields = new ArrayList<RawUdtField>(userDefinedType.getFieldNames().size());
    for (int i = 0; i < userDefinedType.getFieldNames().size(); i++) {
      rawFields.add(
          new RawUdtField(
              userDefinedType.getFieldNames().get(i), userDefinedType.getFieldTypes().get(i)));
    }
    return rawFields;
  }

  /**
   * Method that will convert from user-provided raw map into driver expected {@link UdtValue}
   *
   * <p>Note, See {@link JsonNodeDecoder}, can use tuple format for UDT insertion. E.G. <code>
   * "address": [["city", "beijing"],["postcode", "123456"]]</code>. So the raw value map key can be
   * string or jsonLiteral. But this is highly not recommended and should be avoided.
   */
  private static UdtValue toCqlUdt(
      Map<CqlIdentifier, List<JSONCodec<?, ?>>> fieldsCodecCandidates,
      Map<CqlIdentifier, DataType> fieldTypes,
      UserDefinedType userDefinedType,
      Map<?, ?> rawMapValue)
      throws ToCQLCodecException {

    // raw map key is either String or JsonLiteral of String.
    UdtValue newUdtValue = userDefinedType.newValue();

    // iterate over the raw map entries to populate the expected driver UdtValue
    for (Map.Entry<?, ?> entry : rawMapValue.entrySet()) {

      Object key =
          (entry.getKey() instanceof JsonLiteral<?> jsonLiteralKey)
              ? jsonLiteralKey.value()
              : entry.getKey();

      if (!(key instanceof String fieldName)) {
        throw new ToCQLCodecException(
            key,
            userDefinedType,
            "UDT field name must be a string, but got: %s".formatted(key.getClass().getName()));
      }
      // then we can safely cast it to String
      var fieldIdentifier = CqlIdentifierUtil.cqlIdentifierFromUserInput(fieldName);

      // checked if target field exists
      // throw exception if the field is not present in the UDT schema
      if (!fieldsCodecCandidates.containsKey(fieldIdentifier)) {
        throw new ToCQLCodecException(
            entry.getKey(),
            userDefinedType,
            "Missing field '%s' in UDT '%s'.".formatted(entry.getKey(), userDefinedType.getName()));
      }

      List<JSONCodec<?, ?>> codecCandidates = fieldsCodecCandidates.get(fieldIdentifier);
      JSONCodec<Object, Object> fieldCodec = null;

      Object userInputRawFieldValue =
          entry.getValue() instanceof JsonLiteral<?> jsonLiteral
              ? jsonLiteral.value()
              : entry.getValue();

      DataType expectedFieldType = fieldTypes.get(fieldIdentifier);

      for (JSONCodec<?, ?> codec : codecCandidates) {
        if (codec.handlesJavaValue(userInputRawFieldValue)) {
          fieldCodec = (JSONCodec<Object, Object>) codec;
        }
      }
      // throw exception if no codec found for the user-input field value
      if (fieldCodec == null) {
        String msg =
            String.format(
                "no codec matching in UDT %s for field %s, declared type `%s`, actual type `%s`",
                userDefinedType.getName(),
                entry.getKey(),
                userInputRawFieldValue.getClass().getName(),
                expectedFieldType);
        throw new ToCQLCodecException(userInputRawFieldValue, expectedFieldType, msg);
      }

      Object cqlFieldValue = fieldCodec.toCQL(userInputRawFieldValue);
      if (cqlFieldValue != null) {
        // if user-input field value is null, we skip it, since driver udtValue set() method needs
        // the targetClass.
        newUdtValue =
            newUdtValue.set(
                fieldIdentifier, cqlFieldValue, (Class<Object>) cqlFieldValue.getClass());
      } else {
        // if the value is null, we set it to null in the UdtValue
        newUdtValue = newUdtValue.setToNull(fieldIdentifier);
      }
    }
    return newUdtValue;
  }

  /** Method that will convert from driver-provided CQL UDT type into JSON output. */
  private static JsonNode cqlUdtToJsonNode(
      Map<CqlIdentifier, JSONCodec<?, ?>> fieldCodecs, ObjectMapper objectMapper, Object value)
      throws ToJSONCodecException {

    final ObjectNode result = objectMapper.createObjectNode();
    UdtValue udtValue = (UdtValue) value;
    for (Map.Entry<CqlIdentifier, JSONCodec<?, ?>> fieldToCodecEntry : fieldCodecs.entrySet()) {

      CqlIdentifier fieldIdentifier = fieldToCodecEntry.getKey();
      JSONCodec fieldCodec = fieldToCodecEntry.getValue();

      Object cqlFieldValue = udtValue.getObject(fieldIdentifier);

      switch (cqlFieldValue) {
        case null -> {
          // do nothing, skip null value
        }
        case Collection<?> collection when collection.isEmpty() -> {
          // do nothing, skip empty collection
        }
        case Map<?, ?> map when map.isEmpty() -> {
          // do nothing, skip empty map
        }
        default -> {
          // convert to JSON
          JsonNode jsonValue = fieldCodec.toJSON(objectMapper, cqlFieldValue);
          result.set(fieldIdentifier.asInternal(), jsonValue);
        }
      }
    }
    return result;
  }
}
