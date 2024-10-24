package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToJSONCodecException;
import java.util.*;

/**
 * Container for factories of codecs that handle CQL collection types(Lists and Sets for now).
 * Separated from main {@link JSONCodecs} to keep the code bit more modular.
 */
public abstract class CollectionCodecs {
  private static final GenericType<List<Object>> GENERIC_LIST = GenericType.listOf(Object.class);
  private static final GenericType<Map<String, Object>> GENERIC_MAP =
      GenericType.mapOf(String.class, Object.class);

  /**
   * Factory method to build a codec for a CQL List type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values.
   */
  public static JSONCodec<?, ?> buildToCQLListCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        GENERIC_LIST,
        DataTypes.listOf(elementType),
        (cqlType, value) -> toCQLList(valueCodecs, elementType, value),
        // This code only for to-cql case, not to-json, so we don't need this
        null);
  }

  /**
   * Factory method to build a codec for a CQL Set type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values.
   */
  public static JSONCodec<?, ?> buildToCQLSetCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        // NOTE: although we convert to CQL Set, RowShredder.java binds to Lists
        GENERIC_LIST,
        DataTypes.setOf(elementType),
        (cqlType, value) -> toCQLSet(valueCodecs, elementType, value),
        // This code only for to-cql case, not to-json, so we don't need this
        null);
  }

  /**
   * Factory method to build a codec for a CQL Map type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values. Keys are always strings.
   */
  public static JSONCodec<?, ?> buildToCQLMapCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType keyType, DataType elementType) {
    return new JSONCodec<>(
        GENERIC_MAP,
        DataTypes.mapOf(keyType, elementType),
        (cqlType, value) -> toCQLMap(valueCodecs, elementType, value),
        // This code only for to-cql case, not to-json, so we don't need this
        null);
  }

  public static JSONCodec<?, ?> buildToJsonListCodec(JSONCodec<?, ?> elementCodec) {
    return new JSONCodec<>(
        GENERIC_LIST,
        elementCodec.targetCQLType(), // not exactly correct, but close enough
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) ->
            toJsonNodeFromCollection(elementCodec, objectMapper, value));
  }

  public static JSONCodec<?, ?> buildToJsonSetCodec(JSONCodec<?, ?> elementCodec) {
    return new JSONCodec<>(
        // NOTE: although we convert to CQL Set, RowShredder.java binds to Lists
        GENERIC_LIST,
        elementCodec.targetCQLType(), // not exactly correct, but close enough
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) ->
            toJsonNodeFromCollection(elementCodec, objectMapper, value));
  }

  public static JSONCodec<?, ?> buildToJsonMapCodec(JSONCodec<?, ?> elementCodec) {
    return new JSONCodec<>(
        GENERIC_MAP,
        elementCodec.targetCQLType(),
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) -> toJsonNodeFromMap(elementCodec, objectMapper, value));
  }

  static List<Object> toCQLList(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Collection<?> rawListValue)
      throws ToCQLCodecException {
    Collection<JsonLiteral<?>> listValue = (Collection<JsonLiteral<?>>) rawListValue;
    List<Object> result = new ArrayList<>(listValue.size());

    JSONCodec<Object, Object> elementCodec = null;
    for (JsonLiteral<?> literalElement : listValue) {
      Object element = literalElement.value();
      if (element == null) {
        result.add(null);
        continue;
      }
      if (elementCodec == null || !elementCodec.handlesJavaValue(element)) {
        elementCodec = findCollectionElementCodec(valueCodecs, elementType, element);
      }
      result.add(elementCodec.toCQL(element));
    }
    return result;
  }

  static Set<Object> toCQLSet(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Collection<?> rawSetValue)
      throws ToCQLCodecException {
    Collection<JsonLiteral<?>> setValue = (Collection<JsonLiteral<?>>) rawSetValue;
    // although not mandatory, we use LinkedHashSet to preserve order
    Set<Object> result = new LinkedHashSet<>(setValue.size());
    JSONCodec<Object, Object> elementCodec = null;
    for (JsonLiteral<?> literalElement : setValue) {
      Object element = literalElement.value();
      if (element == null) {
        result.add(null);
        continue;
      }
      if (elementCodec == null || !elementCodec.handlesJavaValue(element)) {
        elementCodec = findCollectionElementCodec(valueCodecs, elementType, element);
      }
      result.add(elementCodec.toCQL(element));
    }
    return result;
  }

  static Map<String, Object> toCQLMap(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Map<?, ?> rawMapValue)
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

  private static JSONCodec<Object, Object> findCollectionElementCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Object element)
      throws ToCQLCodecException {
    for (JSONCodec<?, ?> codec : valueCodecs) {
      if (codec.handlesJavaValue(element)) {
        return (JSONCodec<Object, Object>) codec;
      }
    }
    String msg =
        String.format(
            "no codec matching (list/set) declared element type `%s`, actual value type `%s`",
            elementType, element.getClass());
    throw new ToCQLCodecException(element, elementType, msg);
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
            elementType, element.getClass());
    throw new ToCQLCodecException(element, elementType, msg);
  }

  /**
   * Method that will convert from driver-provided CQL collection (list, set) type into JSON output.
   */
  static JsonNode toJsonNodeFromCollection(
      JSONCodec<?, ?> elementCodec0, ObjectMapper objectMapper, Object collectionValue)
      throws ToJSONCodecException {
    JSONCodec<?, Object> elementCodec = (JSONCodec<?, Object>) elementCodec0;
    final ArrayNode result = objectMapper.createArrayNode();
    for (Object element : (Collection<Object>) collectionValue) {
      if (element == null) {
        result.addNull();
      } else {
        JsonNode jsonValue = elementCodec.toJSON(objectMapper, element);
        result.add(jsonValue);
      }
    }
    return result;
  }

  /** Method that will convert from driver-provided CQL Map type into JSON output. */
  static JsonNode toJsonNodeFromMap(
      JSONCodec<?, ?> elementCodec0, ObjectMapper objectMapper, Object mapValue)
      throws ToJSONCodecException {
    JSONCodec<?, Object> elementCodec = (JSONCodec<?, Object>) elementCodec0;
    final ObjectNode result = objectMapper.createObjectNode();
    for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) mapValue).entrySet()) {
      final String key = String.valueOf(entry.getKey());
      Object value = entry.getValue();
      if (value == null) {
        result.putNull(key);
      } else {
        result.put(key, elementCodec.toJSON(objectMapper, value));
      }
    }
    return result;
  }
}
