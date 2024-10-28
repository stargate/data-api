package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToJSONCodecException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MapCodecs {
  private static final GenericType<Map<String, Object>> GENERIC_MAP =
      GenericType.mapOf(String.class, Object.class);

  /**
   * Factory method to build a codec for a CQL Map type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values. Keys are always strings.
   */
  public static JSONCodec<?, ?> buildToCqlMapCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType keyType, DataType elementType) {
    return new JSONCodec<>(
        GENERIC_MAP,
        DataTypes.mapOf(keyType, elementType),
        (cqlType, value) -> toCqlMap(valueCodecs, elementType, value),
        // This code only for to-cql case, not to-json, so we don't need this
        null);
  }

  public static JSONCodec<?, ?> buildToJsonMapCodec(JSONCodec<?, ?> elementCodec) {
    return new JSONCodec<>(
        GENERIC_MAP,
        elementCodec.targetCQLType(),
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) -> cqlMapToJsonNode(elementCodec, objectMapper, value));
  }

  private static Map<String, Object> toCqlMap(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Map<String, ?> rawMapValue)
      throws ToCQLCodecException {
    Map<String, JsonLiteral<?>> mapValue = (Map<String, JsonLiteral<?>>) rawMapValue;
    Map<String, Object> result = new LinkedHashMap<>(mapValue.size());
    JSONCodec<Object, Object> elementCodec = null;
    for (Map.Entry<String, JsonLiteral<?>> entry : mapValue.entrySet()) {
      String key = entry.getKey();
      Object element = entry.getValue().value();
      if (element == null) {
        result.put(key, null);
        continue;
      }
      // In most cases same codec can be used for all elements, but we still need to check
      // since same CQL value type can have multiple codecs based on JSON value type
      // (like multiple Number representations; or simple Text vs EJSON-wrapper)
      if (elementCodec == null || !elementCodec.handlesJavaValue(element)) {
        elementCodec = findMapValueCodec(valueCodecs, elementType, element);
      }
      result.put(key, elementCodec.toCQL(element));
    }
    return result;
  }

  private static JSONCodec<Object, Object> findMapValueCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Object element)
      throws ToCQLCodecException {
    for (JSONCodec<?, ?> codec : valueCodecs) {
      if (codec.handlesJavaValue(element)) {
        return (JSONCodec<Object, Object>) codec;
      }
    }
    String msg =
        String.format(
            "no codec matching map declared value type `%s`, actual value type `%s`",
            elementType, element.getClass().getName());
    throw new ToCQLCodecException(element, elementType, msg);
  }

  /** Method that will convert from driver-provided CQL Map type into JSON output. */
  private static JsonNode cqlMapToJsonNode(
      JSONCodec<?, ?> valueCodec0, ObjectMapper objectMapper, Object mapValue)
      throws ToJSONCodecException {
    JSONCodec<?, Object> valueCodec = (JSONCodec<?, Object>) valueCodec0;
    final ObjectNode result = objectMapper.createObjectNode();
    for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) mapValue).entrySet()) {
      Object key = entry.getKey();
      if (key instanceof String strKey) {
        Object value = entry.getValue();
        if (value == null) {
          result.putNull(strKey);
        } else {
          result.put(strKey, valueCodec.toJSON(objectMapper, value));
        }
      } else {
        throw new ToJSONCodecException(
            mapValue,
            valueCodec.targetCQLType(),
            String.format(
                "expected String key, got: (%s) %s",
                Optional.ofNullable(key).map(Object::getClass).map(Class::getName).orElse("null"),
                String.valueOf(key)));
      }
    }
    return result;
  }
}
