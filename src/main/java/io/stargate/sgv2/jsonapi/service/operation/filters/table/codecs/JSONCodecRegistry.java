package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
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
public class JSONCodecRegistry {
  private static final GenericType<Object> GENERIC_TYPE_OBJECT = GenericType.of(Object.class);

  /**
   * Dummy codec used to convert a JSON null value in CQL. Same "codec" usable for all CQL types,
   * since null CQL driver needs is just plain Java null.
   */
  private static final JSONCodec<Object, DataType> TO_CQL_NULL_CODEC =
      new JSONCodec<>(
          GENERIC_TYPE_OBJECT,
          /* There's no type for Object so just use something */ DataTypes.BOOLEAN,
          (cqlType, value) -> null,
          null);

  private final Map<DataType, List<JSONCodec<?, ?>>> codecsByCQLType;

  public JSONCodecRegistry(List<JSONCodec<?, ?>> codecs) {
    Objects.requireNonNull(codecs, "codecs must not be null");
    codecsByCQLType = new HashMap<>();
    for (JSONCodec<?, ?> codec : codecs) {
      codecsByCQLType.computeIfAbsent(codec.targetCQLType(), k -> new ArrayList<>()).add(codec);
    }
  }

  /**
   * Returns a codec that can convert a Java object into the object expected by the CQL driver for a
   * specific CQL data type.
   *
   * <p>
   *
   * @param table {@link TableMetadata} to find the column definition in
   * @param column {@link CqlIdentifier} for the column we want to get the codec for.
   * @param value The value to be written to the column
   * @param <JavaT> Type of the Java object we want to convert.
   * @param <CqlT> Type fo the Java object the CQL driver expects.
   * @return The {@link JSONCodec} that can convert the value to the expected type for the column,
   *     or an exception if the codec cannot be found.
   * @throws UnknownColumnException If the column is not found in the table.
   * @throws MissingJSONCodecException If no codec is found for the column and type of the value.
   * @throws ToCQLCodecException If there is a codec for CQL type, but not one for converting from
   *     the Java value type
   */
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, Object value)
      throws UnknownColumnException, MissingJSONCodecException, ToCQLCodecException {

    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(column, "column must not be null");

    var columnMetadata =
        table.getColumn(column).orElseThrow(() -> new UnknownColumnException(table, column));

    // Next, simplify later code by handling nulls directly here (but after column lookup)
    if (value == null) {
      return (JSONCodec<JavaT, CqlT>) TO_CQL_NULL_CODEC;
    }

    // First find candidates for CQL target type in question (if any)
    final DataType columnType = columnMetadata.getType();
    List<JSONCodec<?, ?>> candidates = codecsByCQLType.get(columnType);
    if (candidates == null) { // No scalar codec for this CQL type
      // But maybe structured type?
      if (columnType instanceof ListType lt) {
        List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(lt.getElementType());
        if (valueCodecCandidates != null) {
          // Almost there! But go avoid ClassCastException if input not a JSON Array need this check
          if (!(value instanceof Collection<?>)) {
            throw new ToCQLCodecException(value, columnType, "no codec matching value type");
          }
          return (JSONCodec<JavaT, CqlT>)
              CollectionCodecs.buildToCQLListCodec(valueCodecCandidates, lt.getElementType());
        }

        // fall through
      } else if (columnType instanceof SetType st) {
        List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(st.getElementType());
        if (valueCodecCandidates != null) {
          // Almost there! But go avoid ClassCastException if input not a JSON Array need this check
          if (!(value instanceof Collection<?>)) {
            throw new ToCQLCodecException(value, columnType, "no codec matching value type");
          }
          return (JSONCodec<JavaT, CqlT>)
              CollectionCodecs.buildToCQLSetCodec(valueCodecCandidates, st.getElementType());
        }
        // fall through
      } else if (columnType instanceof VectorType vt) {
        // Only Float<Vector> supported for now
        if (!vt.getElementType().equals(DataTypes.FLOAT)) {
          throw new ToCQLCodecException(value, columnType, "only Vector<Float> supported");
        }
        if (value instanceof Collection<?>) {
          return VectorCodecs.arrayToCQLFloatVectorCodec(vt);
        }
        // !!! TODO: different Codec for Base64 encoded (String) Float vectors

        throw new ToCQLCodecException(value, columnType, "no codec matching value type");
      }

      throw new MissingJSONCodecException(table, columnMetadata, value.getClass(), value);
    }

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
      throw new ToCQLCodecException(value, columnType, "no codec matching value type");
    }
    return match;
  }

  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(
      TableMetadata table, CqlIdentifier columnId)
      throws UnknownColumnException, MissingJSONCodecException {

    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(columnId, "column must not be null");

    var columnMetadata =
        table.getColumn(columnId).orElseThrow(() -> new UnknownColumnException(table, columnId));
    return codecToJSON(table, columnMetadata);
  }

  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(
      TableMetadata table, ColumnMetadata column) throws MissingJSONCodecException {
    // compiler telling me we need to use the unchecked assignment again like the codecFor does
    JSONCodec<JavaT, CqlT> codec = codecToJSON(column.getType());
    if (codec == null) {
      throw new MissingJSONCodecException(table, column, null, null);
    }
    return codec;
  }

  /**
   * Method to find a codec for the specified CQL Type, converting from Java to JSON
   *
   * @param fromCQLType
   * @return Codec to use for conversion, or `null` if none found.
   */
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
      if (valueCodecCandidates == null) {
        return null; // so caller reports problem
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToJsonSetCodec(valueCodecCandidates.get(0));
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
