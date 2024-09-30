package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import java.util.*;

/**
 * Container for factories of codecs that handle CQL collection types(Lists and Sets for now).
 * Seperated from main {@link JSONCodecs} to keep the code bit more modular.
 */
public abstract class CollectionCodecs {
  private static final GenericType<List<Object>> GENERIC_LIST = GenericType.listOf(Object.class);

  /**
   * Factory method to build a codec for a CQL List type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values.
   */
  public static JSONCodec<?, ?> buildListCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        GENERIC_LIST,
        DataTypes.listOf(elementType),
        (cqlType, value) -> toCQLList(valueCodecs, elementType, value),
        (objectMapper, cqlType, value) -> toJsonNode(valueCodecs, objectMapper, cqlType, value));
  }

  /**
   * Factory method to build a codec for a CQL Set type: codec will be given set of possible value
   * codecs (since we have one per input JSON type) and will dynamically select the right one based
   * on the actual element values.
   */
  public static JSONCodec<?, ?> buildSetCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        // NOTE: although we convert to CQL Set, RowShredder.java binds to Lists
        GENERIC_LIST,
        DataTypes.setOf(elementType),
        (cqlType, value) -> toCQLSet(valueCodecs, elementType, value),
        (objectMapper, cqlType, value) -> toJsonNode(valueCodecs, objectMapper, cqlType, value));
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
        elementCodec = findElementCodec(valueCodecs, elementType, element);
      }
      result.add(elementCodec.toCQL(element));
    }
    return result;
  }

  static Set<Object> toCQLSet(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Collection<?> rawSetValue)
      throws ToCQLCodecException {
    Collection<JsonLiteral<?>> setValue = (Collection<JsonLiteral<?>>) rawSetValue;
    Set<Object> result = new HashSet<>(setValue.size());
    JSONCodec<Object, Object> elementCodec = null;
    for (JsonLiteral<?> literalElement : setValue) {
      Object element = literalElement.value();
      if (element == null) {
        result.add(null);
        continue;
      }
      if (elementCodec == null || !elementCodec.handlesJavaValue(element)) {
        elementCodec = findElementCodec(valueCodecs, elementType, element);
      }
      result.add(elementCodec.toCQL(element));
    }
    return result;
  }

  private static JSONCodec<Object, Object> findElementCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Object element)
      throws ToCQLCodecException {
    for (JSONCodec<?, ?> codec : valueCodecs) {
      if (codec.handlesJavaValue(element)) {
        return (JSONCodec<Object, Object>) codec;
      }
    }
    throw new ToCQLCodecException(
        element, elementType, "no codec matching (list/set) element value type");
  }

  static JsonNode toJsonNode(
      List<JSONCodec<?, ?>> valueCodecs,
      ObjectMapper objectMapper,
      DataType fromCQLType,
      Object value) {
    // !!! TO IMPLEMENT
    return null;
  }
}
