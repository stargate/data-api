package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.util.Collection;
import java.util.List;

/**
 * Container for factories of codecs that handle CQL Vector type. Separated from main {@link
 * JSONCodecs} to keep the code somewhat modular.
 */
public abstract class VectorCodecs {
  private static final GenericType<List<Float>> FLOAT_LIST_TYPE = GenericType.listOf(Float.class);
  private static final GenericType<EJSONWrapper> EJSON_TYPE = GenericType.of(EJSONWrapper.class);
  private static final GenericType<float[]> FLOAT_ARRAY_TYPE = new GenericType<>() {};

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> arrayToCQLFloatArrayCodec(
      VectorType vectorType) {
    // Unfortunately we cannot simply construct and return a single Codec instance here
    // because ApiVectorType's dimensions vary, and we need to know the expected dimensions
    // (unless we want to rely on DB validating dimension as part of write and catch failure)
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            FLOAT_LIST_TYPE,
            vectorType,
            (cqlType, value) -> listToCQLFloatArray(vectorType, value),
            // This codec only for to-cql case, not to-json, so we don't need this
            null);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> binaryToCQLFloatArrayCodec(
      VectorType vectorType) {
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            EJSON_TYPE,
            vectorType,
            (cqlType, value) -> binaryToCQLFloatArray(vectorType, value),
            null);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> floatArrayToCQLFloatArrayCodec(
      VectorType vectorType) {
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            FLOAT_ARRAY_TYPE,
            vectorType,
            (cqlType, value) -> floatArrayToCQLFloatArray(vectorType, value),
            null);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> toJSONFloatVectorCodec(VectorType vectorType) {
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            FLOAT_LIST_TYPE,
            vectorType,
            // This codec only for to-json case, not to-cql, so we don't need this
            null,
            (objectMapper, cqlType, value) -> toJsonNode(objectMapper, value));
  }

  /**
   * Method for actual conversion from JSON Number Array into float array for Codec to use as Vector
   * value.
   */
  static float[] listToCQLFloatArray(VectorType vectorType, Collection<?> listValue)
      throws ToCQLCodecException {
    Collection<JsonLiteral<?>> vectorIn = (Collection<JsonLiteral<?>>) listValue;
    validateVectorLength(vectorType, vectorIn, vectorIn.size());

    float[] floats = new float[vectorIn.size()];
    int ix = 0;
    for (JsonLiteral<?> literalElement : vectorIn) {
      Object element = literalElement.value();
      if (element instanceof Number num) {
        floats[ix++] = num.floatValue();
        continue;
      }
      throw new ToCQLCodecException(
          vectorIn,
          vectorType,
          String.format(
              "expected JSON Number value as Vector element at position #%d (of %d), instead have: %s",
              ix, vectorIn.size(), literalElement));
    }
    return floats;
  }

  /**
   * Following the pattern for the other codecs, we have a separate method for actual conversion
   *
   * <p>Does not change the floats buts runs them through the validation for length.
   */
  static float[] floatArrayToCQLFloatArray(VectorType vectorType, float[] floats)
      throws ToCQLCodecException {

    validateVectorLength(vectorType, floats, floats.length);
    return floats;
  }

  /**
   * Method for actual conversion from EJSON-wrapped Base64-encoded String into float array for
   * Codec to use as Vector value.
   */
  static float[] binaryToCQLFloatArray(VectorType vectorType, EJSONWrapper binaryValue)
      throws ToCQLCodecException {
    byte[] binary = JSONCodec.ToCQL.byteArrayFromEJSON(vectorType, binaryValue);
    float[] floats;
    try {
      floats = CqlVectorUtil.bytesToFloats(binary);
    } catch (IllegalArgumentException e) {
      throw new ToCQLCodecException(
          binaryValue,
          vectorType,
          String.format("failed to decode Base64-encoded packed Vector value: %s", e.getMessage()));
    }
    validateVectorLength(vectorType, binaryValue, floats.length);
    return floats;
  }

  private static void validateVectorLength(VectorType vectorType, Object value, int actualLen)
      throws ToCQLCodecException {
    final int expLen = vectorType.getDimensions();
    if (actualLen != expLen) {
      throw new ToCQLCodecException(
          value,
          vectorType,
          String.format(
              "expected vector of length %d, got one with %d elements", expLen, actualLen));
    }
  }

  static JsonNode toJsonNode(ObjectMapper objectMapper, Object vectorValue) {
    // 18-Dec-2024, tatu: [data-api#1775] Support for more efficient but still
    //   allow old binding to work too; test type here, use appropriate logic
    if (vectorValue instanceof float[] floats) {
      return toJsonNodeFromFloats(objectMapper, floats);
    }
    if (vectorValue instanceof CqlVector<?> vector) {
      return toJsonNodeFromCqlVector(objectMapper, (CqlVector<Number>) vector);
    }
    throw new IllegalArgumentException(
        "Unrecognized type for CQL Vector value: " + vectorValue.getClass().getCanonicalName());
  }

  static JsonNode toJsonNodeFromCqlVector(
      ObjectMapper objectMapper, CqlVector<Number> vectorValue) {
    final ArrayNode result = objectMapper.getNodeFactory().arrayNode(vectorValue.size());
    for (Number element : vectorValue) {
      if (element == null) { // is this even legal?
        result.addNull();
      } else {
        result.add(element.floatValue());
      }
    }
    return result;
  }

  static JsonNode toJsonNodeFromFloats(ObjectMapper objectMapper, float[] vectorValue) {
    // For now, output still as array of floats; in future maybe as Base64-encoded packed binary
    final ArrayNode result = objectMapper.getNodeFactory().arrayNode(vectorValue.length);
    for (float f : vectorValue) {
      result.add(f);
    }
    return result;
  }
}
