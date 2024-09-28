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
  ;

  static List<Object> toCQLList(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, List<?> listValue)
      throws ToCQLCodecException {
    List<Object> result = new ArrayList<>(listValue.size());
    JSONCodec<?, ?> elementCodec = null;
    for (Object element : listValue) {
      if (element == null) {
        result.add(null);
        continue;
      }
      if (elementCodec == null || !elementCodec.handlesJavaValue(element)) {
        elementCodec = findElementCodec(valueCodecs, elementType, element);
      }
    }
    return result;
  }

  private static JSONCodec<?, ?> findElementCodec(
      List<JSONCodec<?, ?>> valueCodecs, DataType elementType, Object element)
      throws ToCQLCodecException {
    for (JSONCodec<?, ?> codec : valueCodecs) {
      if (codec.handlesJavaValue(element)) {
        return codec;
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
    // !!! TO IMPLEMENT
    return null;
  }
}
