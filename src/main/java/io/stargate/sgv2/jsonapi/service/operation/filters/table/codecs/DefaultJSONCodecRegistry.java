package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Container of {@link JSONCodec} instances that are used to convert Java objects into the objects
 * expected by the CQL driver for specific CQL data types.
 *
 * <p>Use the default instance from {@link JSONCodecRegistries#DEFAULT_REGISTRY}.
 *
 * <p>See {@link #codecToCQL(TableMetadata, CqlIdentifier, Object)} for the main entry point.
 *
 * <p>IMPORTANT: There must be a codec for every CQL data type we want to write to, even if the
 * translation is an identity translation. This is so we know if the translation can happen, and
 * then if it was done correctly with the actual value. See {@link JSONCodec.ToCQL#unsafeIdentity()}
 * for the identity mapping.
 */
public class DefaultJSONCodecRegistry implements JSONCodecRegistry {
  protected static final GenericType<Object> GENERIC_TYPE_OBJECT = GenericType.of(Object.class);

  /**
   * Dummy codec used to convert a JSON null value in CQL. Same "codec" usable for all CQL types,
   * since null CQL driver needs is just plain Java null.
   */
  protected static final JSONCodec<Object, DataType> TO_CQL_NULL_CODEC =
      new JSONCodec<>(
          GENERIC_TYPE_OBJECT,
          /* There's no type for Object so just use something */ DataTypes.BOOLEAN,
          (cqlType, value) -> null,
          null);

  protected final Map<DataType, List<JSONCodec<?, ?>>> codecsByCQLType;

  public DefaultJSONCodecRegistry(List<JSONCodec<?, ?>> codecs) {
    Objects.requireNonNull(codecs, "codecs must not be null");

    codecsByCQLType = new HashMap<>();
    for (JSONCodec<?, ?> codec : codecs) {
      codecsByCQLType.computeIfAbsent(codec.targetCQLType(), k -> new ArrayList<>()).add(codec);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, Object value)
      throws UnknownColumnException, MissingJSONCodecException, ToCQLCodecException {

    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(column, "column must not be null");

    var columnMetadata =
        table.getColumn(column).orElseThrow(() -> new UnknownColumnException(table, column));
    return codecToCQL(table, column, columnMetadata.getType(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, DataType toCQLType, Object value)
      throws MissingJSONCodecException, ToCQLCodecException {

    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(column, "column must not be null");
    Objects.requireNonNull(toCQLType, "toCQLType must not be null");

    // Next, simplify later code by handling nulls directly here (but after column lookup)
    if (value == null) {
      return (JSONCodec<JavaT, CqlT>) TO_CQL_NULL_CODEC;
    }
    List<JSONCodec<?, ?>> candidates = codecsByCQLType.get(toCQLType);

    if (candidates != null) {
      // this is a scalar type, so we can just use the first codec
      // And if any found try to match with the incoming Java value
      JSONCodec<JavaT, CqlT> match =
          JSONCodec.unchecked(
              candidates.stream()
                  .filter(codec -> codec.handlesJavaValue(value))
                  .findFirst()
                  .orElse(null));
      if (match == null) {
        // Different exception for this case: CQL type supported but not from given Java type
        // (f.ex, CQL Boolean from Java/JSON number)
        throw new ToCQLCodecException(value, toCQLType, "no codec matching value type");
      }
      return match;
    }

    // A CQL collection type
    // these can return Null if they had no candidates, wil throw an exception if they had a
    // candidate
    // but there was a type error
    JSONCodec<JavaT, CqlT> collectionCodec =
        switch (toCQLType) {
          case ListType lt -> codecToCQL(lt, value);
          case SetType st -> codecToCQL(st, value);
          case MapType mt -> codecToCQL(mt, value);
          case VectorType vt -> codecToCQL(vt, value);
          default -> null;
        };

    if (collectionCodec != null) {
      return collectionCodec;
    }
    throw new MissingJSONCodecException(table, column, toCQLType, value.getClass(), value);
  }

  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(ListType listType, Object value)
      throws ToCQLCodecException {

    List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(listType.getElementType());
    if (valueCodecCandidates != null) {
      // Almost there! But to avoid ClassCastException if input not a JSON Array need this check
      if (!(value instanceof Collection<?>)) {
        throw new ToCQLCodecException(value, listType, "no codec matching value type");
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToCqlListCodec(valueCodecCandidates, listType.getElementType());
    }
    return null;
  }

  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(SetType setType, Object value)
      throws ToCQLCodecException {

    List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(setType.getElementType());
    if (valueCodecCandidates != null) {
      // Almost there! But to avoid ClassCastException if input not a JSON Array need this check
      if (!(value instanceof Collection<?>)) {
        throw new ToCQLCodecException(value, setType, "no codec matching value type");
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToCqlSetCodec(valueCodecCandidates, setType.getElementType());
    }
    return null;
  }

  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(MapType mapType, Object value)
      throws ToCQLCodecException {

    List<JSONCodec<?, ?>> keyCodecCandidates = codecsByCQLType.get(mapType.getKeyType());
    List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(mapType.getValueType());
    if (keyCodecCandidates != null && valueCodecCandidates != null) {
      // Almost there! But to avoid ClassCastException if input not a JSON Array need this check
      if (!(value instanceof Map<?, ?>)) {
        throw new ToCQLCodecException(value, mapType, "no codec matching value type");
      }
      return (JSONCodec<JavaT, CqlT>)
          MapCodecs.buildToCqlMapCodec(
              keyCodecCandidates,
              valueCodecCandidates,
              mapType.getKeyType(),
              mapType.getValueType());
    }
    return null;
  }

  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(VectorType vectorType, Object value)
      throws ToCQLCodecException {

    // Only Float<Vector> supported for now
    if (!vectorType.getElementType().equals(DataTypes.FLOAT)) {
      throw new ToCQLCodecException(value, vectorType, "only Vector<Float> supported");
    }
    if (value instanceof Collection<?>) {
      return VectorCodecs.arrayToCQLFloatArrayCodec(vectorType);
    }
    if (value instanceof EJSONWrapper) {
      return VectorCodecs.binaryToCQLFloatArrayCodec(vectorType);
    }
    if (value instanceof float[]) {
      return VectorCodecs.floatArrayToCQLFloatArrayCodec(vectorType);
    }
    throw new ToCQLCodecException(value, vectorType, "no codec matching value type");
  }

  /**
   * Method to find a codec for the specified CQL Type, converting from Java to JSON
   *
   * @param fromCQLType
   * @return Codec to use for conversion, or `null` if none found.
   */
  @Override
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(DataType fromCQLType) {
    List<JSONCodec<?, ?>> candidates = codecsByCQLType.get(fromCQLType);
    if (candidates
        != null) { // Scalar type codecs found: use first one (all have same to-json handling)
      return JSONCodec.unchecked(candidates.get(0));
    }
    // No? Maybe structured type?
    if (fromCQLType instanceof ListType lt) {
      List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(lt.getElementType());
      // Can choose any one of codecs (since to-JSON is same for all); but must get one
      if (valueCodecCandidates == null) {
        return null; // so caller reports problem
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToJsonListCodec(valueCodecCandidates.get(0));
    }
    if (fromCQLType instanceof SetType st) {
      List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(st.getElementType());
      // Can choose any one of codecs (since to-JSON is same for all); but must get one
      if ((valueCodecCandidates == null)) {
        return null; // so caller reports problem
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToJsonSetCodec(valueCodecCandidates.get(0));
    }
    if (fromCQLType instanceof MapType mt) {
      final DataType keyType = mt.getKeyType();
      List<JSONCodec<?, ?>> keyCodecCandidates = codecsByCQLType.get(mt.getKeyType());
      List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(mt.getValueType());
      if (keyCodecCandidates == null || valueCodecCandidates == null) {
        return null; // so caller reports problem
      }
      return (JSONCodec<JavaT, CqlT>)
          MapCodecs.buildToJsonMapCodec(
              keyType, keyCodecCandidates.get(0), valueCodecCandidates.get(0));
    }

    if (fromCQLType instanceof VectorType vt) {
      // Only Float<Vector> supported for now
      if (vt.getElementType().equals(DataTypes.FLOAT)) {
        return VectorCodecs.toJSONFloatVectorCodec(vt);
      }
      // fall through
    }

    return null;
  }
}
