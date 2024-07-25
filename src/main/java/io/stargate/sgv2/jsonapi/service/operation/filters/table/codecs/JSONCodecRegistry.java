package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Builds and manages the {@link JSONCodec} instances that are used to convert Java objects into the
 * objects expected by the CQL driver for specific CQL data types.
 *
 * <p>See {@link #codecFor(TableMetadata, CqlIdentifier, Object)} for the main entry point.
 *
 * <p>IMPORTANT: There must be a codec for every CQL data type we want to write to, even if the
 * translation is an identity translation. This is so we know if the translation can happen, and
 * then if it was done correctly with the actual value. See {@link
 * JSONCodec.FromJava#unsafeIdentity()} for the identity mapping, and example usage in {@link #TEXT}
 * codec.
 */
public class JSONCodecRegistry {

  // Internal list of all codes
  // IMPORTANT: any codec must be added to the list to be available!
  // They are added in a static block at the end of the file
  private static final List<JSONCodec<?, ?>> CODECS;

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
  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecFor(
      TableMetadata table, CqlIdentifier column, Object value)
      throws UnknownColumnException, MissingJSONCodecException {

    Preconditions.checkNotNull(table, "table must not be null");
    Preconditions.checkNotNull(column, "column must not be null");
    Preconditions.checkNotNull(value, "value must not be null");

    // BUG: needs to handle NULl value
    var columnMetadata =
        table.getColumn(column).orElseThrow(() -> new UnknownColumnException(table, column));

    // compiler telling me we need to use the unchecked assignment again like the codecFor does
    JSONCodec<JavaT, CqlT> codec =
        JSONCodec.unchecked(internalCodecFor(columnMetadata.getType(), value));
    if (codec != null) {
      return codec;
    }
    throw new MissingJSONCodecException(table, columnMetadata, value.getClass(), value);
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
  private static JSONCodec<?, ?> internalCodecFor(DataType targetCQLType, Object javaValue) {
    // BUG: needs to handle NULl value
    return CODECS.stream()
        .filter(codec -> codec.test(targetCQLType, javaValue))
        .findFirst()
        .orElse(null);
  }

  // Boolean
  public static final JSONCodec<Boolean, Boolean> BOOLEAN =
      new JSONCodec<>(GenericType.BOOLEAN, DataTypes.BOOLEAN, JSONCodec.FromJava.unsafeIdentity());

  // Numeric Codecs
  public static final JSONCodec<BigDecimal, Long> BIGINT =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.BIGINT,
          JSONCodec.FromJava.safeNumber(BigDecimal::longValueExact));

  public static final JSONCodec<BigDecimal, BigDecimal> DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL, DataTypes.DECIMAL, JSONCodec.FromJava.unsafeIdentity());

  public static final JSONCodec<BigDecimal, Double> DOUBLE =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.DOUBLE,
          JSONCodec.FromJava.safeNumber(BigDecimal::doubleValue));

  public static final JSONCodec<BigDecimal, Float> FLOAT =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.FLOAT,
          JSONCodec.FromJava.safeNumber(BigDecimal::floatValue));

  public static final JSONCodec<BigDecimal, Integer> INT =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.INT,
          JSONCodec.FromJava.safeNumber(BigDecimal::intValueExact));

  public static final JSONCodec<BigDecimal, Short> SMALLINT =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.SMALLINT,
          JSONCodec.FromJava.safeNumber(BigDecimal::shortValueExact));

  public static final JSONCodec<BigDecimal, Byte> TINYINT =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.TINYINT,
          JSONCodec.FromJava.safeNumber(BigDecimal::byteValueExact));

  public static final JSONCodec<BigDecimal, BigInteger> VARINT =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.VARINT,
          JSONCodec.FromJava.safeNumber(BigDecimal::toBigIntegerExact));

  // Text Codecs
  public static final JSONCodec<String, String> ASCII =
      new JSONCodec<>(GenericType.STRING, DataTypes.ASCII, JSONCodec.FromJava.unsafeIdentity());

  public static final JSONCodec<String, String> TEXT =
      new JSONCodec<>(GenericType.STRING, DataTypes.TEXT, JSONCodec.FromJava.unsafeIdentity());

  /** IMPORTANT: All codecs must be added to the list here. */
  static {
    CODECS =
        List.of(
            BOOLEAN, BIGINT, DECIMAL, DOUBLE, FLOAT, INT, SMALLINT, TINYINT, VARINT, ASCII, TEXT);
  }
}
