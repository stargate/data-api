package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Builds and manages the {@link JSONCodec} instances that are used to convert Java objects into the
 * objects expected by the CQL driver for specific CQL data types.
 *
 * <p>See {@link #codecToCQL(TableMetadata, CqlIdentifier, Object)} for the main entry point.
 *
 * <p>IMPORTANT: There must be a codec for every CQL data type we want to write to, even if the
 * translation is an identity translation. This is so we know if the translation can happen, and
 * then if it was done correctly with the actual value. See {@link JSONCodec.ToCQL#unsafeIdentity()}
 * for the identity mapping, and example usage in {@link #TEXT} codec.
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
  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
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
        JSONCodec.unchecked(internalCodecForToCQL(columnMetadata.getType(), value));
    if (codec != null) {
      return codec;
    }
    throw new MissingJSONCodecException(table, columnMetadata, value.getClass(), value);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(
      TableMetadata table, CqlIdentifier columnId)
      throws UnknownColumnException, MissingJSONCodecException {

    Preconditions.checkNotNull(table, "table must not be null");
    Preconditions.checkNotNull(columnId, "column must not be null");

    var columnMetadata =
        table.getColumn(columnId).orElseThrow(() -> new UnknownColumnException(table, columnId));
    return codecToJSON(table, columnMetadata);
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(
      TableMetadata table, ColumnMetadata column) throws MissingJSONCodecException {
    // compiler telling me we need to use the unchecked assignment again like the codecFor does
    JSONCodec<JavaT, CqlT> codec = codecToJSON(column.getType());
    if (codec == null) {
      throw new MissingJSONCodecException(table, column, null, null);
    }
    return codec;
  }

  public static <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(DataType targetCQLType) {
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
  private static JSONCodec<?, ?> internalCodecForToCQL(DataType targetCQLType, Object javaValue) {
    // BUG: needs to handle NULl value
    return CODECS.stream()
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
  private static JSONCodec<?, ?> internalCodecForToJSON(DataType targetCQLType) {
    return CODECS.stream()
        .filter(codec -> codec.testToJSON(targetCQLType))
        .findFirst()
        .orElse(null);
  }

  // Boolean
  private static final JSONCodec<Boolean, Boolean> BOOLEAN =
      new JSONCodec<>(
          GenericType.BOOLEAN,
          DataTypes.BOOLEAN,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::booleanNode));

  // Numeric Codecs
  private static final JSONCodec<BigDecimal, Long> BIGINT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.BIGINT,
          JSONCodec.ToCQL.safeNumber(BigDecimal::longValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<BigInteger, Long> BIGINT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.BIGINT,
          JSONCodec.ToCQL.safeNumber(BigInteger::longValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<Long, Long> BIGINT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.BIGINT,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, BigDecimal> DECIMAL_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.DECIMAL,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<BigInteger, BigDecimal> DECIMAL_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.DECIMAL,
          // This is safe transformation, cannot fail:
          (cqlType, value) -> new BigDecimal(value),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<Long, BigDecimal> DECIMAL_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.DECIMAL,
          // This is safe transformation, cannot fail:
          (cqlType, value) -> new BigDecimal(value),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, Double> DOUBLE_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.DOUBLE,
          // TODO: bounds checks (over/underflow)
          JSONCodec.ToCQL.safeNumber(BigDecimal::doubleValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<BigInteger, Double> DOUBLE_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.DOUBLE,
          // TODO: bounds checks (over/underflow)
          JSONCodec.ToCQL.safeNumber(BigInteger::doubleValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<Long, Double> DOUBLE_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.DOUBLE,
          JSONCodec.ToCQL.safeNumber(Long::doubleValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, Float> FLOAT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.FLOAT,
          // TODO: bounds checks (over/underflow)
          JSONCodec.ToCQL.safeNumber(BigDecimal::floatValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<BigInteger, Float> FLOAT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          // TODO: bounds checks (over/underflow)
          DataTypes.FLOAT,
          JSONCodec.ToCQL.safeNumber(BigInteger::floatValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  private static final JSONCodec<Long, Float> FLOAT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          // TODO: bounds checks (over/underflow)?
          DataTypes.FLOAT,
          JSONCodec.ToCQL.safeNumber(Long::floatValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, Integer> INT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.INT,
          JSONCodec.ToCQL.safeNumber(BigDecimal::intValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigInteger, Integer> INT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.INT,
          JSONCodec.ToCQL.safeNumber(BigInteger::intValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, Integer> INT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.INT,
          JSONCodec.ToCQL.safeNumber(JSONCodec.ToCQL::safeLongToInt),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, Short> SMALLINT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.SMALLINT,
          JSONCodec.ToCQL.safeNumber(BigDecimal::shortValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigInteger, Short> SMALLINT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.SMALLINT,
          JSONCodec.ToCQL.safeNumber(BigInteger::shortValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, Short> SMALLINT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.SMALLINT,
          JSONCodec.ToCQL.safeNumber(JSONCodec.ToCQL::safeLongToSmallint),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, Byte> TINYINT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.TINYINT,
          JSONCodec.ToCQL.safeNumber(BigDecimal::byteValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigInteger, Byte> TINYINT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.TINYINT,
          JSONCodec.ToCQL.safeNumber(BigInteger::byteValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, Byte> TINYINT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.TINYINT,
          JSONCodec.ToCQL.safeNumber(JSONCodec.ToCQL::safeLongToTinyint),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, BigInteger> VARINT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.VARINT,
          JSONCodec.ToCQL.safeNumber(BigDecimal::toBigIntegerExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigInteger, BigInteger> VARINT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.VARINT,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, BigInteger> VARINT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.VARINT,
          JSONCodec.ToCQL.safeNumber(BigInteger::valueOf),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  // Text Codecs
  public static final JSONCodec<String, String> ASCII =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.ASCII,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::textNode));

  public static final JSONCodec<String, String> TEXT =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.TEXT,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::textNode));

  /** IMPORTANT: All codecs must be added to the list here. */
  static {
    CODECS =
        List.of(
            // Numeric Codecs, integer types
            BIGINT_FROM_BIG_DECIMAL,
            BIGINT_FROM_BIG_INTEGER,
            BIGINT_FROM_LONG,
            INT_FROM_BIG_DECIMAL,
            INT_FROM_BIG_INTEGER,
            INT_FROM_LONG,
            SMALLINT_FROM_BIG_DECIMAL,
            SMALLINT_FROM_BIG_INTEGER,
            SMALLINT_FROM_LONG,
            TINYINT_FROM_BIG_DECIMAL,
            TINYINT_FROM_BIG_INTEGER,
            TINYINT_FROM_LONG,
            VARINT_FROM_BIG_DECIMAL,
            VARINT_FROM_BIG_INTEGER,
            VARINT_FROM_LONG,
            // Numeric Codecs, floating-point types
            DECIMAL_FROM_BIG_DECIMAL,
            DECIMAL_FROM_BIG_INTEGER,
            DECIMAL_FROM_LONG,
            DOUBLE_FROM_BIG_DECIMAL,
            DOUBLE_FROM_BIG_INTEGER,
            DOUBLE_FROM_LONG,
            FLOAT_FROM_BIG_DECIMAL,
            FLOAT_FROM_BIG_INTEGER,
            FLOAT_FROM_LONG,
            // Text Codecs
            ASCII,
            TEXT,
            // Other codecs
            BOOLEAN);
  }
}
