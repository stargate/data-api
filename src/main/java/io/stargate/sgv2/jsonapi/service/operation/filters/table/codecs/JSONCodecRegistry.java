package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import java.util.ArrayList;
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

    Preconditions.checkNotNull(table, "table must not be null");
    Preconditions.checkNotNull(column, "column must not be null");

    var columnMetadata =
        table.getColumn(column).orElseThrow(() -> new UnknownColumnException(table, column));

    // First find candidates for CQL target type in question (if any)
    List<JSONCodec<?, ?>> candidates = codecsByCQLType.get(columnMetadata.getType());
    if (candidates == null) { // No codec for this CQL type
      throw new MissingJSONCodecException(
          table, columnMetadata, (value == null) ? null : value.getClass(), value);
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
      throw new ToCQLCodecException(
          value, columnMetadata.getType(), "no codec matching value type");
    }
    return match;
  }

  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(
      TableMetadata table, CqlIdentifier columnId)
      throws UnknownColumnException, MissingJSONCodecException {

    Preconditions.checkNotNull(table, "table must not be null");
    Preconditions.checkNotNull(columnId, "column must not be null");

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
    return (candidates == null) ? null : JSONCodec.unchecked(candidates.get(0));
  }
}
