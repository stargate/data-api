package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import java.util.*;

public abstract class CollectionCodecs {
  private static final GenericType<List<Object>> GENERIC_LIST = GenericType.listOf(Object.class);

  public static JSONCodec<?, ?> buildListCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        GENERIC_LIST,
        DataTypes.listOf(elementType),
        (cqlType, value) -> toCQLList(valueCodecs, elementType, value),
        (objectMapper, cqlType, value) -> toJsonNode(valueCodecs, objectMapper, cqlType, value));
  }

  public static JSONCodec<?, ?> buildSetCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType) {
    return new JSONCodec<>(
        // NOTE: although we convert to Sets, RowShredder.java binds to Lists
        GENERIC_LIST,
        DataTypes.setOf(elementType),
        (cqlType, value) -> toCQLSet(valueCodecs, elementType, value),
        (objectMapper, cqlType, value) -> toJsonNode(valueCodecs, objectMapper, cqlType, value));
  }

  static List<Object> toCQLList(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Collection<?> listValue)
      throws ToCQLCodecException {
    List<Object> result = new ArrayList<>(listValue.size());
    JSONCodec<Object, Object> elementCodec = null;
    for (Object element : listValue) {
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
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Collection<?> setValue)
      throws ToCQLCodecException {
    Set<Object> result = new HashSet<>(setValue.size());
    JSONCodec<Object, Object> elementCodec = null;
    for (Object element : setValue) {
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
    throw new ToCQLCodecException(element, elementType, "no codec matching value type");
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
