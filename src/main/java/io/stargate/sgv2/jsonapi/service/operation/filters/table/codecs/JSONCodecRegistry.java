package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/**
 * Builds and manages the {@link JSONCodec} instances that are used to convert Java objects into the
 * objects expected by the CQL driver for specific CQL data types.
 *
 * <p>See {@link #codecFor(DataType, Object)}
 *
 * <p>IMPORTANT: There must be a codec for every CQL data type we want to write to, even if the
 * translation is an identity translation. This is so we know if the translation can happen, and
 * then if it was done correctly with the actual value. See {@link
 * JSONCodec.FromJava#unsafeIdentity()} for the identity mapping, and example usage in {@link #TEXT}
 * codec.
 */
public class JSONCodecRegistry {

  // Internal list of all codecs
  // IMPORTANT: any codec must be added to the list to be available to {@ink #codecFor(DataType,
  // Object)}
  // They are added in a static block at the end of the file
  private static final List<JSONCodec<?, ?>> CODECS;

  /**
   * Returns a codec that can convert a Java object into the object expected by the CQL driver for a
   * specific CQL data type.
   *
   * <p>
   *
   * @param targetCQLType CQL type of the target column we want to write to.
   * @param javaValue Java object that we want to write to the column.
   * @return Optional of the codec that can convert the Java object into the object expected by the
   *     CQL driver. If no codec is found, the optional is empty.
   * @param <JavaT> Type of the Java object we want to convert.
   * @param <CqlT> Type fo the Java object the CQL driver expects.
   */
  public static <JavaT, CqlT> Optional<JSONCodec<JavaT, CqlT>> codecFor(
      DataType targetCQLType, JavaT javaValue) {

    return Optional.ofNullable(
        JSONCodec.unchecked(
            CODECS.stream()
                .filter(codec -> codec.test(targetCQLType, javaValue))
                .findFirst()
                .orElse(null)));
  }

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
    CODECS = List.of(BIGINT, DECIMAL, DOUBLE, FLOAT, INT, SMALLINT, TINYINT, VARINT, ASCII, TEXT);
  }
}
