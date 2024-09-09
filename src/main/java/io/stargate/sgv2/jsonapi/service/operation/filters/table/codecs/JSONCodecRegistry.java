package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import java.util.List;
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

  private final List<JSONCodec<?, ?>> codecs;

  JSONCodecRegistry(List<JSONCodec<?, ?>> codecs) {
    this.codecs = Objects.requireNonNull(codecs, "codecs must not be null");
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
   */
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, Object value)
      throws UnknownColumnException, MissingJSONCodecException {

    Preconditions.checkNotNull(table, "table must not be null");
    Preconditions.checkNotNull(column, "column must not be null");

    var columnMetadata =
        table.getColumn(column).orElseThrow(() -> new UnknownColumnException(table, column));

    // compiler telling me we need to use the unchecked assignment again like the codecFor does
    JSONCodec<JavaT, CqlT> codec =
        JSONCodec.unchecked(internalCodecForToCQL(columnMetadata.getType(), value));
    if (codec != null) {
      return codec;
    }
    throw new MissingJSONCodecException(table, columnMetadata, value.getClass(), value);
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

  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(DataType targetCQLType) {
    return JSONCodec.unchecked(internalCodecForToJSON(targetCQLType));
  }

  /**
   * Internal only method to find a codec for the specified type and value.
   *
   * <p>The return type is {@code JSONCodec<?, ?>} because type erasure means that returning {@code
   * JSONCodec<JavaT, CqlT>} would be erased. Therefore, we need to use {@link JSONCodec#unchecked}
   * anyway, which results in this method returning {@code <?, ?>}. However, you are guaranteed that
   * it will match the types you wanted, due to the call to the codec to test.
   *
   * @param targetCQLType
   * @param javaValue
   * @return The codec, or `null` if none found.
   */
  private JSONCodec<?, ?> internalCodecForToCQL(DataType targetCQLType, Object javaValue) {
    // BUG: needs to handle NULl value
    return codecs.stream()
        .filter(codec -> codec.testToCQL(targetCQLType, javaValue))
        .findFirst()
        .orElse(null);
  }

  /**
   * Same as {@link #internalCodecForToCQL(DataType, Object)}
   *
   * @param targetCQLType
   * @return
   */
  private JSONCodec<?, ?> internalCodecForToJSON(DataType targetCQLType) {
    return codecs.stream()
        .filter(codec -> codec.testToJSON(targetCQLType))
        .findFirst()
        .orElse(null);
  }
}
