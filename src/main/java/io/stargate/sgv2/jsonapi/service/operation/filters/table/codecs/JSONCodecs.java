package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

/**
 * Defines the {@link JSONCodec} instances that are added to the {@link
 * JSONCodecRegistries#DEFAULT_REGISTRY} in that class.
 *
 * <p><b>NOTE:</b> Not enough to just define the codecs here, they must also be listed in {@link
 * JSONCodecRegistries#DEFAULT_REGISTRY}
 */
public abstract class JSONCodecs {

  // Boolean
  public static final JSONCodec<Boolean, Boolean> BOOLEAN =
      new JSONCodec<>(
          GenericType.BOOLEAN,
          DataTypes.BOOLEAN,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::booleanNode));

  // Blob/binary
  public static final JSONCodec<EJSONWrapper, ByteBuffer> BINARY =
      new JSONCodec<>(
          GenericType.of(EJSONWrapper.class),
          DataTypes.BLOB,
          JSONCodec.ToCQL::byteBufferFromEJSON,
          (mapper, cqlType, value) -> EJSONWrapper.binaryWrapper(value).asJsonNode());

  // Numeric Codecs
  public static final JSONCodec<BigDecimal, Long> BIGINT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.BIGINT,
          JSONCodec.ToCQL.safeNumber(BigDecimal::longValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigInteger, Long> BIGINT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.BIGINT,
          JSONCodec.ToCQL.safeNumber(BigInteger::longValueExact),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, Long> BIGINT_FROM_LONG =
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

  public static final JSONCodec<BigInteger, BigDecimal> DECIMAL_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.DECIMAL,
          // This is safe transformation, cannot fail:
          (cqlType, value) -> new BigDecimal(value),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, BigDecimal> DECIMAL_FROM_LONG =
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

  public static final JSONCodec<BigInteger, Double> DOUBLE_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          DataTypes.DOUBLE,
          // TODO: bounds checks (over/underflow)
          JSONCodec.ToCQL.safeNumber(BigInteger::doubleValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, Double> DOUBLE_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          DataTypes.DOUBLE,
          JSONCodec.ToCQL.safeNumber(Long::doubleValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  // Codec needed to support "not-a-number" values: encoded as Strings in JSON
  public static final JSONCodec<String, Double> DOUBLE_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.DOUBLE,
          JSONCodec.ToCQL::doubleFromString,
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigDecimal, Float> FLOAT_FROM_BIG_DECIMAL =
      new JSONCodec<>(
          GenericType.BIG_DECIMAL,
          DataTypes.FLOAT,
          // TODO: bounds checks (over/underflow)
          JSONCodec.ToCQL.safeNumber(BigDecimal::floatValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<BigInteger, Float> FLOAT_FROM_BIG_INTEGER =
      new JSONCodec<>(
          GenericType.BIG_INTEGER,
          // TODO: bounds checks (over/underflow)
          DataTypes.FLOAT,
          JSONCodec.ToCQL.safeNumber(BigInteger::floatValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  public static final JSONCodec<Long, Float> FLOAT_FROM_LONG =
      new JSONCodec<>(
          GenericType.LONG,
          // TODO: bounds checks (over/underflow)?
          DataTypes.FLOAT,
          JSONCodec.ToCQL.safeNumber(Long::floatValue),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::numberNode));

  // Codec needed to support "not-a-number" values: encoded as Strings in JSON
  public static final JSONCodec<String, Float> FLOAT_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.FLOAT,
          JSONCodec.ToCQL::floatFromString,
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

  // Date/time Codecs

  public static final JSONCodec<String, LocalDate> DATE_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.DATE,
          JSONCodec.ToCQL.safeFromString(LocalDate::parse),
          JSONCodec.ToJSON.toJSONUsingToString());

  public static final JSONCodec<String, CqlDuration> DURATION_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.DURATION,
          // CqlDuration.from() accepts 2 formats; ISO-8601 ("P1H30M") and "1h30m" (Cassandra
          // compact) format
          JSONCodec.ToCQL.safeFromString(CqlDuration::from),
          // But since we must produce ISO-8601, need custom JSON serialization
          (objectMapper, fromCQLType, value) ->
              objectMapper
                  .getNodeFactory()
                  .textNode(CqlDurationConverter.toISO8601Duration(value)));

  public static final JSONCodec<String, LocalTime> TIME_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.TIME,
          JSONCodec.ToCQL.safeFromString(LocalTime::parse),
          JSONCodec.ToJSON.toJSONUsingToString());

  public static final JSONCodec<String, Instant> TIMESTAMP_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.TIMESTAMP,
          JSONCodec.ToCQL.safeFromString(Instant::parse),
          JSONCodec.ToJSON.toJSONUsingToString());

  public static final JSONCodec<EJSONWrapper, Instant> TIMESTAMP_FROM_EJSON =
      new JSONCodec<>(
          GenericType.of(EJSONWrapper.class),
          DataTypes.TIMESTAMP,
          JSONCodec.ToCQL::instantFromEJSON,
          JSONCodec.ToJSON.toJSONUsingToString());

  // Text Codecs
  public static final JSONCodec<String, String> ASCII =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.ASCII,
          JSONCodec.ToCQL::safeAscii,
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::textNode));

  public static final JSONCodec<String, String> TEXT =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.TEXT,
          JSONCodec.ToCQL.unsafeIdentity(),
          JSONCodec.ToJSON.unsafeNodeFactory(JsonNodeFactory.instance::textNode));

  // UUID Codecs

  public static final JSONCodec<String, java.util.UUID> UUID_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.UUID,
          JSONCodec.ToCQL.safeFromString(UUID::fromString),
          JSONCodec.ToJSON.toJSONUsingToString());

  // While not allowed to be created as column type, we do support reading/writing
  // of columns of this type in existing tables.
  public static final JSONCodec<String, java.util.UUID> TIMEUUID_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.TIMEUUID,
          JSONCodec.ToCQL.safeFromString(UUID::fromString),
          JSONCodec.ToJSON.toJSONUsingToString());

  // Misc other Codecs
  public static final JSONCodec<String, java.net.InetAddress> INET_FROM_STRING =
      new JSONCodec<>(
          GenericType.STRING,
          DataTypes.INET,
          JSONCodec.ToCQL.safeFromString(JSONCodec.ToCQL::inetAddressFromString),
          (objectMapper, fromCQLType, value) ->
              objectMapper.getNodeFactory().textNode(value.getHostAddress()));
}
