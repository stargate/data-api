package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapCodecs {
  private static final GenericType<Map<Object, Object>> GENERIC_MAP =
      GenericType.mapOf(Object.class, Object.class);

  /**
   * Factory method to build a codec for a CQL Map type: codec will be given set of possible key and
   * value codecs (since we have one per input JSON type) and will dynamically select the right one
   * based on the actual element keys and values.
   */
  public static JSONCodec<?, ?> buildToCqlMapCodec(
      List<JSONCodec<?, ?>> keyCodecs,
      List<JSONCodec<?, ?>> valueCodecs,
      DataType keyType,
      DataType elementType) {
    return new JSONCodec<>(
        GENERIC_MAP,
        DataTypes.mapOf(keyType, elementType),
        (cqlType, value) -> toCqlMap(keyCodecs, valueCodecs, keyType, elementType, value),
        // This code only for to-cql case, not to-json, so we don't need this
        null);
  }

  public static JSONCodec<?, ?> buildToJsonMapCodec(
      DataType keyType, JSONCodec<?, ?> keyCodec, JSONCodec<?, ?> elementCodec) {
    return new JSONCodec<>(
        GENERIC_MAP,
        elementCodec.targetCQLType(),
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) ->
            cqlMapToJsonNode(keyType, keyCodec, elementCodec, objectMapper, value));
  }

  /**
   * Method that will convert from user-provided raw Map into Cql Map.
   *
   * <p>Map key can be string or non-string type. String key of the raw map will be String,
   * non-string key will be JsonLiteral.
   *
   * @param keyCodecs List of codecs that can handle the keys of the map
   * @param valueCodecs List of codecs that can handle the values of the map
   * @param keyType Map column CQL type of the key
   * @param elementType Map column CQL type of the value
   * @param rawMapValue User-provided raw map value
   */
  private static Map<Object, Object> toCqlMap(
      List<JSONCodec<?, ?>> keyCodecs,
      List<JSONCodec<?, ?>> valueCodecs,
      DataType keyType,
      DataType elementType,
      Map<?, ?> rawMapValue)
      throws ToCQLCodecException {
    Map<?, JsonLiteral<?>> mapValue = (Map<?, JsonLiteral<?>>) rawMapValue;
    Map<Object, Object> result = new LinkedHashMap<>(mapValue.size());
    JSONCodec<Object, Object> keyCodec = null;
    JSONCodec<Object, Object> elementCodec = null;
    for (Map.Entry<?, JsonLiteral<?>> entry : mapValue.entrySet()) {
      // Map key can be string or non-string type. String key of the raw map will be String,
      // non-string key will be JsonLiteral.
      Object key =
          (entry.getKey() instanceof JsonLiteral<?> jsonLiteralKey)
              ? jsonLiteralKey.value()
              : entry.getKey();
      Object element = entry.getValue().value();
      if (element == null) {
        result.put(key, null);
        continue;
      }
      // In most cases same codec can be used for all elements, but we still need to check
      // since same CQL value type can have multiple codecs based on JSON value type
      // (like multiple Number representations; or simple Text vs EJSON-wrapper)
      if (elementCodec == null || !elementCodec.handlesJavaValue(element)) {
        elementCodec = findMapKeyOrValueCodec(valueCodecs, elementType, element, true);
      }
      if (keyCodec == null || !keyCodec.handlesJavaValue(key)) {
        keyCodec = findMapKeyOrValueCodec(keyCodecs, keyType, key, false);
      }
      result.put(keyCodec.toCQL(key), elementCodec.toCQL(element));
    }
    return result;
  }

  /**
   * Find the codec that can handle the given map key or value.
   *
   * @param codecs List of codes that may handle the given map key or value
   * @param type key type or value type
   * @param target the user provided raw map key/value
   * @param isValue true indicating that we are looking for a value codec, false indicating that we
   *     are looking for a key codec
   */
  private static JSONCodec<Object, Object> findMapKeyOrValueCodec(
      List<JSONCodec<?, ?>> codecs, DataType type, Object target, boolean isValue)
      throws ToCQLCodecException {
    for (JSONCodec<?, ?> codec : codecs) {
      if (codec.handlesJavaValue(target)) {
        return (JSONCodec<Object, Object>) codec;
      }
    }
    var keyOrValue = isValue ? "value" : "key";
    List<String> codecDescs = codecs.stream().map(codec -> codec.javaType().toString()).toList();
    String msg =
        String.format(
            "no codec matching map declared %s type `%s`, actual type `%s` (checked %d codecs: %s)",
            keyOrValue, type, target.getClass().getName(), codecDescs.size(), codecDescs);
    throw new ToCQLCodecException(target, type, msg);
  }

  /**
   * Method that will convert from driver-provided CQL Map type into JSON output.
   *
   * <p>On map read path, it wil in object format when map column is on text/ascii keys, return in
   * tuple format for other key types. E.G. <ui>
   * <li>Map<Double,Double> cql->json [[1.0, 1.0], [2.0, 2.0]]
   * <li>Map<UUID, TEXT> cql-json [["d3b07384-d113-4c5a-9d3d-92b3a4d1f5a3", "apple"],
   *     [[d3b07384-d113-4c5a-9d3d-92b3a4d1f5a4", "banana"]]
   * <li>Map<TEXT, TEXT> cql-json {"apple": "apple", "banana": "banana"}
   * <li>Map<ASCII, ASCII> cql-json {"apple": "apple", "banana": "banana"} </ui>
   *
   * @param keyType Cql map key type
   * @param keyCodec Codec that can handle the keys of the map
   * @param valueCodec Codec that can handle the values of the map
   * @param objectMapper Jackson object mapper
   * @param mapValue Cql map value
   * @return JsonNode representation of the Cql map
   */
  private static JsonNode cqlMapToJsonNode(
      DataType keyType,
      JSONCodec<?, ?> keyCodec,
      JSONCodec<?, ?> valueCodec,
      ObjectMapper objectMapper,
      Object mapValue)
      throws ToJSONCodecException {
    JSONCodec<?, Object> keyCodecCasted = (JSONCodec<?, Object>) keyCodec;
    JSONCodec<?, Object> valueCodecCasted = (JSONCodec<?, Object>) valueCodec;

    if (keyType.equals(DataTypes.TEXT) || keyType.equals(DataTypes.ASCII)) {
      final ObjectNode result = objectMapper.createObjectNode();
      for (Map.Entry<String, Object> entry : ((Map<String, Object>) mapValue).entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        if (value == null) {
          result.putNull(key);
        } else {
          result.set(key, valueCodecCasted.toJSON(objectMapper, value));
        }
      }
      return result;
    }
    final ArrayNode result = objectMapper.createArrayNode();
    for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) mapValue).entrySet()) {
      final ArrayNode tupleEntry = objectMapper.createArrayNode();
      tupleEntry.add(keyCodecCasted.toJSON(objectMapper, entry.getKey()));
      if (entry.getValue() == null) {
        tupleEntry.addNull();
      } else {
        tupleEntry.add(valueCodecCasted.toJSON(objectMapper, entry.getValue()));
      }
      result.add(tupleEntry);
    }
    return result;
  }
}
