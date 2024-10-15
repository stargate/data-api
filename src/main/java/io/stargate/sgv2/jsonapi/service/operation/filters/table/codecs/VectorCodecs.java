package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
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
  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> arrayToCQLFloatVectorCodec(
      VectorType vectorType) {
    // Unfortunately we cannot simply construct and return a single Codec instance here
    // because VectorType's dimensions vary, and we need to know the expected dimensions
    // (unless we want to rely on DB validating dimension as part of write and catch failure)
    return (JSONCodec<JavaT, CqlT>)
        new JSONCodec<>(
            // NOTE: although we convert to CqlVector, RowShredder.java binds to Lists
            GenericType.listOf(Float.class),
            vectorType,
            (cqlType, value) -> toCQLFloatVector(vectorType, value),
            // This code only for to-cql case, not to-json, so we don't need this
            null);
  }

  static CqlVector<Float> toCQLFloatVector(VectorType vectorType, Collection<?> listValue)
      throws ToCQLCodecException {
    Collection<JsonLiteral<?>> vectorIn = (Collection<JsonLiteral<?>>) listValue;
    final int expLen = vectorType.getDimensions();
    if (expLen != vectorIn.size()) {
      throw new ToCQLCodecException(
          vectorIn, vectorType, "expected vector of length " + expLen + ", got " + vectorIn.size());
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
          "expected Number value as Vector element #" + floats.size() + ", got: " + literalElement);
    }
    return CqlVector.newInstance(floats);
  }
}
