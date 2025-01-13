package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.quarkus.logging.Log;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToJSONCodecException;
import java.util.*;

/**
 * Container for factories of codecs that handle CQL collection types(Lists and Sets for now).
 * Separated from main {@link JSONCodecs} to keep the code bit more modular.
 */
public abstract class CollectionCodecs {
  private static final GenericType<List<Object>> GENERIC_LIST = GenericType.listOf(Object.class);

  /**
   * Factory method to build a codec for a CQL List type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values.
   */
  public static JSONCodec<?, ?> buildToCqlListCodec(
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
  public static JSONCodec<?, ?> buildToCqlSetCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        // NOTE: although we convert to CQL Set, RowShredder.java binds to Lists
        GENERIC_LIST,
        DataTypes.setOf(elementType),
        (cqlType, value) -> toCQLSet(valueCodecs, elementType, value),
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
            cqlCollectionToJsonNode(elementCodec, objectMapper, value));
  }

  public static JSONCodec<?, ?> buildToJsonSetCodec(JSONCodec<?, ?> elementCodec) {
    return new JSONCodec<>(
        // NOTE: although we convert to CQL Set, RowShredder.java binds to Lists
        GENERIC_LIST,
        elementCodec.targetCQLType(), // not exactly correct, but close enough
        // This code only for to-json case, not to-cql, so we don't need this
        null,
        (objectMapper, cqlType, value) ->
            cqlCollectionToJsonNode(elementCodec, objectMapper, value));
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
      Log.error("find elementCoded "  + elementCodec);
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

  private static JSONCodec<Object, Object> findCollectionElementCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Object element)
      throws ToCQLCodecException {
    for (JSONCodec<?, ?> codec : valueCodecs) {
      if (codec.handlesJavaValue(element)) {
        return (JSONCodec<Object, Object>) codec;
      }
    }
    List<String> codecDescs =
        valueCodecs.stream().map(codec -> codec.javaType().toString()).toList();
    String msg =
        String.format(
            "no codec matching (list/set) declared element type `%s`, actual value type `%s` (checked %d codecs: %s)",
            elementType, element.getClass().getName(), codecDescs.size(), codecDescs);
    throw new ToCQLCodecException(element, elementType, msg);
  }

  /**
   * Method that will convert from driver-provided CQL collection (list, set) type into JSON output.
   */
  static JsonNode cqlCollectionToJsonNode(
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
}
