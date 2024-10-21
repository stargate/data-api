package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Container for factories of codecs that handle CQL Vector type. Separated from main {@link
 * JSONCodecs} to keep the code somewhat modular.
 */
public abstract class VectorCodecs {
  private static final GenericType<List<Float>> FLOAT_LIST_TYPE = GenericType.listOf(Float.class);
  private static final GenericType<EJSONWrapper> EJSON_TYPE = GenericType.of(EJSONWrapper.class);

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> arrayToCQLFloatVectorCodec(
      VectorType vectorType) {
    // Unfortunately we cannot simply construct and return a single Codec instance here
    // because VectorType's dimensions vary, and we need to know the expected dimensions
    // (unless we want to rely on DB validating dimension as part of write and catch failure)
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            FLOAT_LIST_TYPE,
            vectorType,
            (cqlType, value) -> toCQLFloatVector(vectorType, value),
            // This codec only for to-cql case, not to-json, so we don't need this
            null);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> binaryToCQLFloatVectorCodec(
      VectorType vectorType) {
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            EJSON_TYPE,
            vectorType,
            null,
            // (cqlType, value) -> toCQLFloatVector(vectorType, value),
            // This codec only for to-cql case, not to-json, so we don't need this
            null);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> toJSONFloatVectorCodec(VectorType vectorType) {
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            FLOAT_LIST_TYPE,
            vectorType,
            // This codec only for to-json case, not to-cql, so we don't need this
            null,
            (objectMapper, cqlType, value) -> toJsonNode(objectMapper, (CqlVector<Number>) value));
  }

  static CqlVector<Float> toCQLFloatVector(VectorType vectorType, Collection<?> listValue)
      throws ToCQLCodecException {
    Collection<JsonLiteral<?>> vectorIn = (Collection<JsonLiteral<?>>) listValue;
    final int expLen = vectorType.getDimensions();
    if (expLen != vectorIn.size()) {
      throw new ToCQLCodecException(
          vectorIn,
          vectorType,
          "expected vector of length "
              + expLen
              + ", got one with "
              + vectorIn.size()
              + " elements");
    }
    List<Float> floats = new ArrayList<>(expLen);
    for (JsonLiteral<?> literalElement : vectorIn) {
      Object element = literalElement.value();
      if (element instanceof Number num) {
        floats.add(num.floatValue());
        continue;
      }
      throw new ToCQLCodecException(
          vectorIn,
          vectorType,
          String.format(
              "expected JSON Number value as Vector element at position #%d (of %d), instead have: %s",
              floats.size(), expLen, literalElement));
    }
    return CqlVector.newInstance(floats);
  }

  static JsonNode toJsonNode(ObjectMapper objectMapper, CqlVector<Number> vectorValue) {
    final ArrayNode result = objectMapper.createArrayNode();
    for (Number element : vectorValue) {
      if (element == null) { // is this even legal?
        result.addNull();
      } else {
        result.add(element.floatValue());
      }
    }
    return result;
  }
}
