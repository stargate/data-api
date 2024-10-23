package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import io.stargate.sgv2.jsonapi.util.Base64Util;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JSONCodecRegistryTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonNodeFactory JSONS = OBJECT_MAPPER.getNodeFactory();
  private static final JSONCodecRegistryTestData TEST_DATA = new JSONCodecRegistryTestData();

  /** Helper to get a codec when we only care about the CQL type and the fromValue */
  private <JavaT, CqlT> JSONCodec<JavaT, CqlT> assertGetCodecToCQL(
      DataType cqlType, Object fromValue) {
    return assertGetCodecToCQL(cqlType, TEST_DATA.COLUMN_NAME, fromValue);
  }

  /**
   * Helper to get a codec when we only care about the CQL type, Column name, and the fromValue
   *
   * <p>Asserts the codec says it can work with the CQL type and the fromValue class
   */
  private <JavaT, CqlT> JSONCodec<JavaT, CqlT> assertGetCodecToCQL(
      DataType cqlType, CqlIdentifier columnName, Object fromValue) {
    JSONCodec<JavaT, CqlT> codec =
        assertDoesNotThrow(
            () ->
                JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
                    TEST_DATA.mockTableMetadata(cqlType), columnName, fromValue),
            String.format(
                "Get codec for cqlType=%s and fromValue.class=%s",
                cqlType, fromValue.getClass().getName()));
    assertThat(codec)
        .isNotNull()
        .satisfies(
            c -> {
              assertThat(c.targetCQLType())
                  .as("Codec supports the target type " + cqlType)
                  .isEqualTo(cqlType);
              assertThat(c.javaType().getRawType())
                  .as("Codec supports the fromValue class " + fromValue.getClass().getName())
                  .isAssignableFrom(fromValue.getClass());
            });
    return codec;
  }

  private <JavaT, CqlT> JSONCodec<JavaT, CqlT> assertGetCodecToJSON(DataType cqlType) {
    JSONCodec<JavaT, CqlT> codec = JSONCodecRegistries.DEFAULT_REGISTRY.codecToJSON(cqlType);

    assertThat(codec).isNotNull();
    return codec;
  }

  /**
   * @param cqlType The type of the CQL column we are going to test
   * @param fromValue The value we got from Jackson and want to store in CQL
   * @param expectedCqlValue The value we expect to pass to the CQL driver
   */
  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesInt")
  public void codecToCQLInts(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesFloat")
  public void codecToCQLFloats(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesText")
  public void codecToCQLText(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesDatetime")
  public void codecToCQLDatetime(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesUuid")
  public void codecToCQLUuid(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesOther")
  public void codecToCQLOther(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesCollections")
  public void codecToCQLCollections(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesMaps")
  public void codecToCQLMaps(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToCQLTestCasesVectors")
  public void codecToCQLVectors(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    _codecToCQL(cqlType, fromValue, expectedCqlValue);
  }

  private void _codecToCQL(DataType cqlType, Object fromValue, Object expectedCqlValue) {
    var codec = assertGetCodecToCQL(cqlType, fromValue);

    var actualCqlValue =
        assertDoesNotThrow(
            () -> codec.toCQL(fromValue),
            String.format(
                "Calling codec for cqlType=%s and fromValue.class=%s",
                cqlType, fromValue.getClass().getName()));

    // Can only check exact class for non-Container types, for List/Set/Map need
    // to use looser type check
    final Class<?> expectedCqlValueType = cleanseType(expectedCqlValue.getClass());

    assertThat(actualCqlValue)
        .as(
            "Comparing expected and actual CQL value for fromValue.class=%s and fromValue.toString()=%s",
            fromValue.getClass().getName(), fromValue.toString())
        .isInstanceOfAny(expectedCqlValueType)
        .isEqualTo(expectedCqlValue);
  }

  private Class<?> cleanseType(Class<?> clazz) {
    if (List.class.isAssignableFrom(clazz)) {
      return List.class;
    }
    if (Set.class.isAssignableFrom(clazz)) {
      return Set.class;
    }
    if (Map.class.isAssignableFrom(clazz)) {
      return Map.class;
    }
    return clazz;
  }

  private static Stream<Arguments> validCodecToCQLTestCasesInt() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql)
    // Note: all Numeric types accept 3 Java types: Long, BigInteger, BigDecimal
    return Stream.of(
        // Integer types:
        Arguments.of(DataTypes.BIGINT, -456L, -456L),
        Arguments.of(DataTypes.BIGINT, BigInteger.valueOf(123), 123L),
        Arguments.of(DataTypes.BIGINT, BigDecimal.valueOf(999.0), 999L),
        Arguments.of(DataTypes.INT, -42000L, -42000),
        Arguments.of(DataTypes.INT, BigInteger.valueOf(19000), 19000),
        Arguments.of(DataTypes.INT, BigDecimal.valueOf(23456.0), 23456),
        Arguments.of(DataTypes.SMALLINT, -3999L, (short) -3999),
        Arguments.of(DataTypes.SMALLINT, BigInteger.valueOf(1234), (short) 1234),
        Arguments.of(DataTypes.SMALLINT, BigDecimal.valueOf(10911.0), (short) 10911),
        Arguments.of(DataTypes.TINYINT, -39L, (byte) -39),
        Arguments.of(DataTypes.TINYINT, BigInteger.valueOf(123), (byte) 123),
        Arguments.of(DataTypes.TINYINT, BigDecimal.valueOf(109.0), (byte) 109),
        Arguments.of(DataTypes.VARINT, -39999L, BigInteger.valueOf(-39999)),
        Arguments.of(DataTypes.VARINT, BigInteger.valueOf(1), BigInteger.valueOf(1)),
        Arguments.of(
            DataTypes.VARINT, BigDecimal.valueOf(123456789.0), BigInteger.valueOf(123456789)));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesFloat() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql)
    // Note: all Numeric types accept 3 Java types: Long, BigInteger, BigDecimal; and
    // 2 accept String as well (for Not-a-Numbers)
    return Stream.of(
        Arguments.of(DataTypes.DECIMAL, 123L, BigDecimal.valueOf(123L)),
        Arguments.of(DataTypes.DECIMAL, BigInteger.valueOf(34567L), BigDecimal.valueOf(34567L)),
        Arguments.of(DataTypes.DECIMAL, BigDecimal.valueOf(0.25), BigDecimal.valueOf(0.25)),
        Arguments.of(DataTypes.DOUBLE, 123L, Double.valueOf(123L)),
        Arguments.of(DataTypes.DOUBLE, BigInteger.valueOf(34567L), Double.valueOf(34567L)),
        Arguments.of(DataTypes.DOUBLE, BigDecimal.valueOf(0.25), Double.valueOf(0.25)),
        Arguments.of(DataTypes.FLOAT, 123L, Float.valueOf(123L)),
        Arguments.of(DataTypes.FLOAT, BigInteger.valueOf(34567L), Float.valueOf(34567L)),
        Arguments.of(DataTypes.FLOAT, BigDecimal.valueOf(0.25), Float.valueOf(0.25f)),
        // Floating-point types: not-a-numbers
        Arguments.of(DataTypes.DOUBLE, "NaN", Double.NaN),
        Arguments.of(DataTypes.DOUBLE, "Infinity", Double.POSITIVE_INFINITY),
        Arguments.of(DataTypes.DOUBLE, "-Infinity", Double.NEGATIVE_INFINITY),
        Arguments.of(DataTypes.FLOAT, "NaN", Float.NaN),
        Arguments.of(DataTypes.FLOAT, "Infinity", Float.POSITIVE_INFINITY),
        Arguments.of(DataTypes.FLOAT, "-Infinity", Float.NEGATIVE_INFINITY));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesText() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql)
    // Textual types: ASCII, TEXT (VARCHAR is an alias for TEXT).
    return Stream.of(
        Arguments.of(DataTypes.ASCII, TEST_DATA.STRING_ASCII_SAFE, TEST_DATA.STRING_ASCII_SAFE),
        Arguments.of(DataTypes.TEXT, TEST_DATA.STRING_ASCII_SAFE, TEST_DATA.STRING_ASCII_SAFE),
        Arguments.of(
            DataTypes.TEXT,
            TEST_DATA.STRING_WITH_2BYTE_UTF8_CHAR,
            TEST_DATA.STRING_WITH_2BYTE_UTF8_CHAR),
        Arguments.of(
            DataTypes.TEXT,
            TEST_DATA.STRING_WITH_3BYTE_UTF8_CHAR,
            TEST_DATA.STRING_WITH_3BYTE_UTF8_CHAR));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesDatetime() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql)
    // Date/time types: DATE, DURATION, TIME, TIMESTAMP
    return Stream.of(
        Arguments.of(
            DataTypes.DATE, TEST_DATA.DATE_VALID_STR, LocalDate.parse(TEST_DATA.DATE_VALID_STR)),
        Arguments.of(
            DataTypes.DURATION,
            TEST_DATA.DURATION_VALID1_STR,
            CqlDuration.from(TEST_DATA.DURATION_VALID1_STR)),
        Arguments.of(
            DataTypes.DURATION,
            TEST_DATA.DURATION_VALID2_STR,
            CqlDuration.from(TEST_DATA.DURATION_VALID2_STR)),
        Arguments.of(
            DataTypes.TIME, TEST_DATA.TIME_VALID_STR, LocalTime.parse(TEST_DATA.TIME_VALID_STR)),
        Arguments.of(
            DataTypes.TIMESTAMP,
            TEST_DATA.TIMESTAMP_VALID_STR,
            Instant.parse(TEST_DATA.TIMESTAMP_VALID_STR)));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesUuid() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql)
    return Stream.of(
        Arguments.of(
            DataTypes.UUID,
            TEST_DATA.UUID_VALID_STR_LC,
            java.util.UUID.fromString(TEST_DATA.UUID_VALID_STR_LC)),
        Arguments.of(
            DataTypes.UUID,
            TEST_DATA.UUID_VALID_STR_UC,
            java.util.UUID.fromString(TEST_DATA.UUID_VALID_STR_UC)),
        Arguments.of(
            DataTypes.TIMEUUID,
            TEST_DATA.UUID_VALID_STR_LC,
            java.util.UUID.fromString(TEST_DATA.UUID_VALID_STR_LC)),
        Arguments.of(
            DataTypes.TIMEUUID,
            TEST_DATA.UUID_VALID_STR_UC,
            java.util.UUID.fromString(TEST_DATA.UUID_VALID_STR_UC)));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesOther() throws Exception {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql)
    return Stream.of(
        // Short regular base64-encoded string
        Arguments.of(
            DataTypes.BLOB,
            binaryWrapper(TEST_DATA.BASE64_PADDED_ENCODED_STR),
            ByteBuffer.wrap(TEST_DATA.BASE64_PADDED_DECODED_BYTES)),
        // edge case: empty String -> byte[0]
        Arguments.of(DataTypes.BLOB, binaryWrapper(""), ByteBuffer.wrap(new byte[0])),
        Arguments.of(
            DataTypes.INET,
            TEST_DATA.INET_ADDRESS_VALID_STRING,
            InetAddress.getByName(TEST_DATA.INET_ADDRESS_VALID_STRING)));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesCollections() {
    // Arguments: (CQL-type, from-caller-json, bound-by-driver-for-cql)
    return Stream.of(
        // // Lists:
        Arguments.of(
            DataTypes.listOf(DataTypes.TEXT),
            Arrays.asList(stringLiteral("a"), stringLiteral("b"), nullLiteral()),
            Arrays.asList("a", "b", null)),
        Arguments.of(
            DataTypes.listOf(DataTypes.INT),
            // Important: all incoming JSON numbers are represented as Long, BigInteger,
            // or BigDecimal. But CQL column here requires ints (not longs)
            Arrays.asList(numberLiteral(123L), numberLiteral(-42L), nullLiteral()),
            Arrays.asList(123, -42, null)),
        Arguments.of(
            DataTypes.listOf(DataTypes.DOUBLE),
            // All JSON fps bound as BigDecimal:
            Arrays.asList(
                numberLiteral(new BigDecimal(0.25)),
                numberLiteral(new BigDecimal(-7.5)),
                nullLiteral()),
            Arrays.asList(0.25, -7.5, null)),

        // // Sets:
        Arguments.of(
            DataTypes.setOf(DataTypes.TEXT),
            Arrays.asList(stringLiteral("a"), stringLiteral("b")),
            Set.of("a", "b")),
        Arguments.of(
            DataTypes.setOf(DataTypes.INT),
            Arrays.asList(numberLiteral(123L), numberLiteral(-42L)),
            Set.of(123, -42)),
        Arguments.of(
            DataTypes.setOf(DataTypes.DOUBLE),
            // All JSON fps bound as BigDecimal:
            Arrays.asList(
                numberLiteral(new BigDecimal(-0.75)), numberLiteral(new BigDecimal(42.5))),
            Set.of(-0.75, 42.5)));
  }

  private static Stream<Arguments> validCodecToCQLTestCasesMaps() {
    // Arguments: (CQL-type, from-caller-json, bound-by-driver-for-cql)
    return Stream.of(
        Arguments.of(
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT),
            Map.of("str1", stringLiteral("a"), "str2", stringLiteral("b")),
            Map.of("str1", "a", "str2", "b")),
        Arguments.of(
            DataTypes.mapOf(DataTypes.ASCII, DataTypes.INT),
            // Important: all incoming JSON numbers are represented as Long, BigInteger,
            // or BigDecimal. But CQL column here requires ints (not longs)
            Map.of("numA", numberLiteral(123L), "numB", numberLiteral(-42L)),
            Map.of("numA", 123, "numB", -42)),
        Arguments.of(
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE),
            // All JSON fps bound as BigDecimal:
            Map.of(
                "fp1",
                numberLiteral(new BigDecimal(0.25)),
                "fp2",
                numberLiteral(new BigDecimal(-7.5))),
            Map.of("fp1", 0.25, "fp2", -7.5)));
  }

  //

  private static Stream<Arguments> validCodecToCQLTestCasesVectors() {
    DataType vector3Type = DataTypes.vectorOf(DataTypes.FLOAT, 3);
    float[] rawFloats3 = new float[] {0.0f, -0.5f, 0.25f};
    byte[] packedFloats3 = CqlVectorUtil.floatsToBytes(rawFloats3);

    DataType vector4Type = DataTypes.vectorOf(DataTypes.FLOAT, 4);
    float[] rawFloats4 = new float[] {1.0f, 0.0f, 100.75f, -1.0f};
    byte[] packedFloats4 = CqlVectorUtil.floatsToBytes(rawFloats4);

    // Arguments: (CQL-type, from-caller-json, bound-by-driver-for-cql)
    return Stream.of(
        // First: Array of Numbers representation
        Arguments.of(
            vector3Type,
            // Important: all incoming JSON numbers are represented as Long, BigInteger,
            // or BigDecimal. All legal as source for Float.
            Arrays.asList(
                numberLiteral(0L),
                numberLiteral(new BigDecimal(-0.5)),
                numberLiteral(new BigDecimal(0.25))),
            CqlVectorUtil.floatsToCqlVector(rawFloats3)),
        // Second: Base64-encoded representation (Base64 of 4-byte "packed" float values)
        Arguments.of(
            vector3Type, binaryWrapper(packedFloats3), CqlVectorUtil.floatsToCqlVector(rawFloats3)),
        Arguments.of(
            vector4Type,
            binaryWrapper(packedFloats4),
            CqlVectorUtil.floatsToCqlVector(rawFloats4)));
  }

  private static JsonLiteral<Number> numberLiteral(Number value) {
    return new JsonLiteral<>(value, JsonType.NUMBER);
  }

  private static JsonLiteral<String> stringLiteral(String value) {
    return new JsonLiteral<>(value, JsonType.STRING);
  }

  private static JsonLiteral<String> nullLiteral() {
    return new JsonLiteral<>(null, JsonType.NULL);
  }

  private static EJSONWrapper binaryWrapper(byte[] binary) {
    return binaryWrapper(Base64Util.encodeAsMimeBase64(binary));
  }

  private static EJSONWrapper binaryWrapper(String base64Encoded) {
    return new EJSONWrapper(
        EJSONWrapper.EJSONType.BINARY, JsonNodeFactory.instance.textNode(base64Encoded));
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesInt")
  public void codecToJSONInts(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesFloat")
  public void codecToJSONFloats(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesText")
  public void codecToJSONText(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesDatetime")
  public void codecToJSONDatetime(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesUuid")
  public void codecToJSONUuid(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesOther")
  public void codecToJSONOther(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesCollections")
  public void codecToJSONCollections(
      DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  @ParameterizedTest
  @MethodSource("validCodecToJSONTestCasesVectors")
  public void codecToJSONVectors(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    _codecToJSON(cqlType, fromValue, expectedJsonValue);
  }

  private void _codecToJSON(DataType cqlType, Object fromValue, JsonNode expectedJsonValue) {
    var codec = assertGetCodecToJSON(cqlType);

    JsonNode actualJSONValue =
        assertDoesNotThrow(
            () -> codec.toJSON(OBJECT_MAPPER, fromValue),
            String.format(
                "Calling codec for cqlType=%s and fromValue.class=%s",
                cqlType, fromValue.getClass().getName()));

    assertThat(actualJSONValue)
        .as(
            "Comparing expected and actual JsonNode value for fromValue.class=%s and fromValue.toString()=%s",
            fromValue.getClass().getName(), fromValue.toString())
        .isInstanceOfAny(expectedJsonValue.getClass())
        .matches(
            n -> jsonNodesMatch(actualJSONValue, expectedJsonValue),
            String.format(
                "Expected JSON value:%s, actual JSON value: %s",
                expectedJsonValue, actualJSONValue));
  }

  // Helper method because of cases where JsonNode.equals() is not enough (Blob/binary)
  private static boolean jsonNodesMatch(JsonNode actual, JsonNode expected) {
    // NOTE: we can't compare JsonNode directly, as in some cases exact node type
    // is different (e.g. Long vs BigInteger or byte[] vs String for BLOB)
    return expected.equals(actual) || expected.toString().equals(actual.toString());
  }

  private static Stream<Arguments> validCodecToJSONTestCasesInt() {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    // Note: driver does not return different value types for any single CQL type
    return Stream.of(
        // Integer types:
        Arguments.of(DataTypes.BIGINT, -123456890L, JSONS.numberNode(-123456890L)),
        Arguments.of(DataTypes.INT, -42000, JSONS.numberNode(-42000)),
        Arguments.of(DataTypes.SMALLINT, (short) -3999, JSONS.numberNode((short) -3999)),
        Arguments.of(DataTypes.TINYINT, (byte) -39, JSONS.numberNode((byte) -39)),
        Arguments.of(
            DataTypes.VARINT,
            BigInteger.valueOf(-39999L),
            JSONS.numberNode(BigInteger.valueOf(-39999))));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesFloat() {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        Arguments.of(
            DataTypes.DECIMAL,
            BigDecimal.valueOf(0.25),
            JSONS.numberNode(BigDecimal.valueOf(0.25))),
        Arguments.of(DataTypes.DOUBLE, 0.25, JSONS.numberNode(0.25)),
        Arguments.of(DataTypes.FLOAT, 0.25f, JSONS.numberNode(0.25f)),
        // Floating-point types: not-a-numbers
        Arguments.of(DataTypes.DOUBLE, Double.NaN, JSONS.numberNode(Double.NaN)),
        Arguments.of(
            DataTypes.DOUBLE, Double.POSITIVE_INFINITY, JSONS.numberNode(Double.POSITIVE_INFINITY)),
        Arguments.of(
            DataTypes.DOUBLE, Double.NEGATIVE_INFINITY, JSONS.numberNode(Double.NEGATIVE_INFINITY)),
        Arguments.of(DataTypes.FLOAT, Float.NaN, JSONS.numberNode(Float.NaN)),
        Arguments.of(
            DataTypes.FLOAT, Float.POSITIVE_INFINITY, JSONS.numberNode(Float.POSITIVE_INFINITY)),
        Arguments.of(
            DataTypes.FLOAT, Float.NEGATIVE_INFINITY, JSONS.numberNode(Float.NEGATIVE_INFINITY)));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesText() {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        Arguments.of(
            DataTypes.ASCII,
            TEST_DATA.STRING_ASCII_SAFE,
            JSONS.textNode(TEST_DATA.STRING_ASCII_SAFE)),
        Arguments.of(
            DataTypes.TEXT,
            TEST_DATA.STRING_ASCII_SAFE,
            JSONS.textNode(TEST_DATA.STRING_ASCII_SAFE)),
        Arguments.of(
            DataTypes.TEXT,
            TEST_DATA.STRING_WITH_2BYTE_UTF8_CHAR,
            JSONS.textNode(TEST_DATA.STRING_WITH_2BYTE_UTF8_CHAR)),
        Arguments.of(
            DataTypes.TEXT,
            TEST_DATA.STRING_WITH_3BYTE_UTF8_CHAR,
            JSONS.textNode(TEST_DATA.STRING_WITH_3BYTE_UTF8_CHAR)));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesDatetime() {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        Arguments.of(
            DataTypes.DATE,
            LocalDate.parse(TEST_DATA.DATE_VALID_STR),
            JSONS.textNode(TEST_DATA.DATE_VALID_STR)),
        Arguments.of(
            DataTypes.DURATION,
            CqlDuration.from(TEST_DATA.DURATION_VALID1_STR),
            JSONS.textNode(TEST_DATA.DURATION_VALID1_STR)),
        Arguments.of(
            DataTypes.TIME,
            LocalTime.parse(TEST_DATA.TIME_VALID_STR),
            JSONS.textNode(TEST_DATA.TIME_VALID_STR)),
        Arguments.of(
            DataTypes.TIMESTAMP,
            Instant.parse(TEST_DATA.TIMESTAMP_VALID_STR),
            JSONS.textNode(TEST_DATA.TIMESTAMP_VALID_STR)));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesUuid() {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        // Short regular base64-encoded string
        Arguments.of(
            DataTypes.UUID,
            UUID.fromString(TEST_DATA.UUID_VALID_STR_LC),
            JSONS.textNode(TEST_DATA.UUID_VALID_STR_LC)),
        Arguments.of(
            DataTypes.UUID,
            UUID.fromString(TEST_DATA.UUID_VALID_STR_UC),
            // JSON codec accepts either casing but always writes lowercase UUIDs
            JSONS.textNode(TEST_DATA.UUID_VALID_STR_UC.toLowerCase())),
        Arguments.of(
            DataTypes.TIMEUUID,
            UUID.fromString(TEST_DATA.UUID_VALID_STR_LC),
            JSONS.textNode(TEST_DATA.UUID_VALID_STR_LC)),
        Arguments.of(
            DataTypes.TIMEUUID,
            UUID.fromString(TEST_DATA.UUID_VALID_STR_UC),
            // JSON codec accepts either casing but always writes lowercase UUIDs
            JSONS.textNode(TEST_DATA.UUID_VALID_STR_UC.toLowerCase())));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesOther() throws Exception {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        // Short regular base64-encoded string
        Arguments.of(
            DataTypes.BLOB,
            ByteBuffer.wrap(TEST_DATA.BASE64_PADDED_DECODED_BYTES),
            binaryWrapper(TEST_DATA.BASE64_PADDED_ENCODED_STR).asJsonNode()),

        // edge case: empty String -> byte[0]
        Arguments.of(DataTypes.BLOB, ByteBuffer.wrap(new byte[0]), binaryWrapper("").asJsonNode()),
        Arguments.of(
            DataTypes.INET,
            InetAddress.getByName(TEST_DATA.INET_ADDRESS_VALID_STRING),
            JSONS.textNode(TEST_DATA.INET_ADDRESS_VALID_STRING)));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesCollections() throws IOException {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        // // Lists:
        Arguments.of(
            DataTypes.listOf(DataTypes.TEXT),
            Arrays.asList("a", "b", null),
            OBJECT_MAPPER.readTree("[\"a\",\"b\",null]")),
        Arguments.of(
            DataTypes.listOf(DataTypes.INT),
            Arrays.asList(123, -42, null),
            OBJECT_MAPPER.readTree("[123,-42,null]")),
        Arguments.of(
            DataTypes.listOf(DataTypes.DOUBLE),
            Arrays.asList(0.25, -4.5, null),
            OBJECT_MAPPER.readTree("[0.25,-4.5,null]")),
        Arguments.of(
            DataTypes.listOf(DataTypes.BLOB),
            Arrays.asList(ByteBuffer.wrap(TEST_DATA.BASE64_PADDED_DECODED_BYTES)),
            OBJECT_MAPPER
                .createArrayNode()
                .add(binaryWrapper(TEST_DATA.BASE64_PADDED_ENCODED_STR).asJsonNode())),

        // // Sets:
        Arguments.of(
            DataTypes.setOf(DataTypes.TEXT),
            new LinkedHashSet<>(Arrays.asList("a", "b")),
            OBJECT_MAPPER.readTree("[\"a\",\"b\"]")),
        Arguments.of(
            DataTypes.setOf(DataTypes.INT),
            new LinkedHashSet<>(Arrays.asList(123, -42)),
            OBJECT_MAPPER.readTree("[123,-42]")),
        Arguments.of(
            DataTypes.setOf(DataTypes.DOUBLE),
            new LinkedHashSet<>(Arrays.asList(0.25, -4.5)),
            OBJECT_MAPPER.readTree("[0.25,-4.5]")));
  }

  private static Stream<Arguments> validCodecToJSONTestCasesVectors() throws IOException {
    // Arguments: (CQL-type, from-CQL-result-set, JsonNode-to-serialize)
    return Stream.of(
        Arguments.of(
            DataTypes.vectorOf(DataTypes.FLOAT, 2),
            CqlVector.newInstance(0.25f, -0.5f),
            OBJECT_MAPPER.readTree("[0.25,-0.5]")),
        Arguments.of(
            DataTypes.vectorOf(DataTypes.FLOAT, 3),
            CqlVector.newInstance(0.25f, -0.5f, 1.0f),
            OBJECT_MAPPER.readTree("[0.25,-0.5,1.0]")));
  }

  @Test
  public void missingJSONCodecException() {

    var error =
        assertThrowsExactly(
            MissingJSONCodecException.class,
            () ->
                JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
                    TEST_DATA.mockTableMetadata(TEST_DATA.UNSUPPORTED_CQL_DATA_TYPE),
                    TEST_DATA.COLUMN_NAME,
                    TEST_DATA.RANDOM_STRING),
            String.format(
                "Get codec for unsupported CQL type %s", TEST_DATA.UNSUPPORTED_CQL_DATA_TYPE));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.table.getName()).isEqualTo(TEST_DATA.TABLE_NAME);
              assertThat(e.column.getType())
                  .as("Column type of error is " + TEST_DATA.UNSUPPORTED_CQL_DATA_TYPE)
                  .isEqualTo(TEST_DATA.UNSUPPORTED_CQL_DATA_TYPE);
              assertThat(e.column.getName()).isEqualTo(TEST_DATA.COLUMN_NAME);
              assertThat(e.javaType).isEqualTo(TEST_DATA.RANDOM_STRING.getClass());
              assertThat(e.value).isEqualTo(TEST_DATA.RANDOM_STRING);

              assertThat(e.getMessage())
                  .contains(TEST_DATA.TABLE_NAME.asInternal())
                  .contains(TEST_DATA.COLUMN_NAME.asInternal())
                  .contains(TEST_DATA.UNSUPPORTED_CQL_DATA_TYPE.toString())
                  .contains(TEST_DATA.RANDOM_STRING.getClass().getName())
                  .contains(TEST_DATA.RANDOM_STRING);
            });
  }

  @Test
  public void unknownColumnException() {

    var error =
        assertThrowsExactly(
            UnknownColumnException.class,
            () ->
                JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
                    TEST_DATA.mockTableMetadata(DataTypes.TEXT),
                    TEST_DATA.RANDOM_CQL_IDENTIFIER,
                    TEST_DATA.RANDOM_STRING),
            String.format(
                "Get codec for unknown column named '%s'", TEST_DATA.RANDOM_CQL_IDENTIFIER));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.table.getName()).isEqualTo(TEST_DATA.TABLE_NAME);
              assertThat(e.column).isEqualTo(TEST_DATA.RANDOM_CQL_IDENTIFIER);

              assertThat(e.getMessage())
                  .contains(TEST_DATA.TABLE_NAME.asInternal())
                  .contains(TEST_DATA.RANDOM_CQL_IDENTIFIER.asInternal());
            });
  }

  @ParameterizedTest
  @MethodSource("outOfRangeOfCqlNumberTestCases")
  public void outOfRangeOfCqlNumber(DataType typeToTest, Number valueToTest, String rootCause) {
    assertToCQLFail(
        typeToTest,
        valueToTest,
        typeToTest.toString(),
        valueToTest.getClass().getName(),
        valueToTest.toString(),
        "Root cause: " + rootCause);
  }

  private static Stream<Arguments> outOfRangeOfCqlNumberTestCases() {
    // Arguments: (DataType, Number-outside-range)
    return Stream.of(
        Arguments.of(DataTypes.BIGINT, TEST_DATA.OUT_OF_RANGE_FOR_BIGINT, "Overflow"),
        Arguments.of(
            DataTypes.BIGINT,
            TEST_DATA.OUT_OF_RANGE_FOR_BIGINT.toBigIntegerExact(),
            "BigInteger out of long range"),
        Arguments.of(DataTypes.INT, TEST_DATA.OVERFLOW_FOR_INT, "Overflow"),
        Arguments.of(
            DataTypes.INT,
            TEST_DATA.OVERFLOW_FOR_INT.toBigIntegerExact(),
            "BigInteger out of int range"),
        Arguments.of(DataTypes.INT, TEST_DATA.OVERFLOW_FOR_INT.longValueExact(), "Overflow"),
        Arguments.of(DataTypes.INT, TEST_DATA.UNDERFLOW_FOR_INT.longValueExact(), "Underflow"),
        Arguments.of(DataTypes.SMALLINT, TEST_DATA.OVERFLOW_FOR_SMALLINT, "Overflow"),
        Arguments.of(
            DataTypes.SMALLINT,
            TEST_DATA.OVERFLOW_FOR_SMALLINT.toBigIntegerExact(),
            "BigInteger out of short range"),
        Arguments.of(
            DataTypes.SMALLINT, TEST_DATA.OVERFLOW_FOR_SMALLINT.longValueExact(), "Overflow"),
        Arguments.of(
            DataTypes.SMALLINT, TEST_DATA.UNDERFLOW_FOR_SMALLINT.longValueExact(), "Underflow"),
        Arguments.of(DataTypes.TINYINT, TEST_DATA.OVERFLOW_FOR_TINYINT, "Overflow"),
        Arguments.of(
            DataTypes.TINYINT,
            TEST_DATA.OVERFLOW_FOR_TINYINT.toBigIntegerExact(),
            "BigInteger out of byte range"),
        Arguments.of(
            DataTypes.TINYINT, TEST_DATA.OVERFLOW_FOR_TINYINT.longValueExact(), "Overflow"),
        Arguments.of(
            DataTypes.TINYINT, TEST_DATA.UNDERFLOW_FOR_TINYINT.longValueExact(), "Underflow"));
  }

  @ParameterizedTest
  @MethodSource("nonExactToCqlIntegerTestCases")
  public void nonExactToCqlInteger(DataType typeToTest, Number valueToTest) {
    assertToCQLFail(
        typeToTest,
        valueToTest,
        typeToTest.toString(),
        valueToTest.getClass().getName(),
        valueToTest.toString(),
        "Root cause: Rounding necessary");
  }

  private static Stream<Arguments> nonExactToCqlIntegerTestCases() {
    // Arguments: (DataType, Number-not-exact-as-integer)
    return Stream.of(
        Arguments.of(DataTypes.BIGINT, TEST_DATA.NOT_EXACT_AS_INTEGER),
        Arguments.of(DataTypes.INT, TEST_DATA.NOT_EXACT_AS_INTEGER),
        Arguments.of(DataTypes.SMALLINT, TEST_DATA.NOT_EXACT_AS_INTEGER),
        Arguments.of(DataTypes.TINYINT, TEST_DATA.NOT_EXACT_AS_INTEGER),
        Arguments.of(DataTypes.VARINT, TEST_DATA.NOT_EXACT_AS_INTEGER));
  }

  @ParameterizedTest
  @MethodSource("nonAsciiValueFailTestCases")
  public void nonAsciiValueFail(String valueToTest) {
    assertToCQLFail(
        DataTypes.ASCII,
        valueToTest,
        valueToTest.getClass().getName(),
        valueToTest.toString(),
        "Root cause: String contains non-ASCII character at index");
  }

  private static Stream<Arguments> nonAsciiValueFailTestCases() {
    return Stream.of(
        Arguments.of(TEST_DATA.STRING_WITH_2BYTE_UTF8_CHAR),
        Arguments.of(TEST_DATA.STRING_WITH_3BYTE_UTF8_CHAR),
        Arguments.of(TEST_DATA.STRING_WITH_4BYTE_SURROGATE_CHAR));
  }

  @ParameterizedTest
  @MethodSource("invalidCodecToCQLTestCasesDatetime")
  public void invalidDatetimeValueFail(DataType cqlDatetimeType, String valueToTest) {
    var codec = assertGetCodecToCQL(cqlDatetimeType, valueToTest);

    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            String.format(
                "Throw ToCQLCodecException when attempting to convert `%s` from non-ASCII value %s",
                cqlDatetimeType, valueToTest));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(cqlDatetimeType);
              assertThat(e.value).isEqualTo(valueToTest);

              assertThat(e.getMessage())
                  .contains(cqlDatetimeType.toString())
                  .contains(valueToTest.getClass().getName())
                  .contains(valueToTest.toString())
                  .contains("Root cause: Invalid String value for type");
            });
  }

  private static Stream<Arguments> invalidCodecToCQLTestCasesDatetime() {
    // Arguments: (CQL-type, from-caller)
    // Date/time types: DATE, DURATION, TIME, TIMESTAMP
    return Stream.of(
        Arguments.of(DataTypes.DATE, TEST_DATA.DATE_INVALID_STR),
        Arguments.of(DataTypes.DURATION, TEST_DATA.DURATION_INVALID_STR),
        Arguments.of(DataTypes.TIME, TEST_DATA.TIME_INVALID_STR),
        Arguments.of(DataTypes.TIMESTAMP, TEST_DATA.TIMESTAMP_INVALID_STR));
  }

  // difficult to parameterize this test, so just test a few cases
  @Test
  public void invalidBinaryInputs() {
    assertToCQLFail(
        DataTypes.BLOB,
        new EJSONWrapper(
            EJSONWrapper.EJSONType.BINARY, JsonNodeFactory.instance.textNode("bad-base64!")),
        "Root cause: Unsupported JSON value in EJSON $binary wrapper: String not valid Base64-encoded");

    assertToCQLFail(
        DataTypes.BLOB,
        new EJSONWrapper(EJSONWrapper.EJSONType.BINARY, JsonNodeFactory.instance.numberNode(42)),
        "Root cause: Unsupported JSON value type in EJSON $binary wrapper (NUMBER): only STRING allowed");

    // We require Base64 padding
    assertToCQLFail(
        DataTypes.BLOB,
        binaryWrapper(TEST_DATA.BASE64_UNPADDED_ENCODED_STR),
        "Unexpected end of base64-encoded String",
        "expects padding");
  }

  @Test
  public void invalidInetAddress() {
    assertToCQLFail(
        DataTypes.INET,
        TEST_DATA.INET_ADDRESS_INVALID_STRING,
        "Root cause: Invalid String value for type `INET`",
        "Invalid IP address value");
  }

  @Test
  public void invalidListValueFail() {
    DataType cqlTypeToTest = DataTypes.listOf(DataTypes.INT);
    List<JsonLiteral<?>> valueToTest = List.of(stringLiteral("abc"));
    var codec = assertGetCodecToCQL(cqlTypeToTest, new ArrayList<>());

    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            "Throw ToCQLCodecException when attempting to convert List<INT> from List<TEXT>");

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.getMessage()).contains("no codec matching (list/set)");
            });
  }

  @Test
  public void invalidSetValueFail() {
    DataType cqlTypeToTest = DataTypes.setOf(DataTypes.INT);
    List<JsonLiteral<?>> valueToTest = List.of(stringLiteral("xyz"));
    var codec = assertGetCodecToCQL(cqlTypeToTest, valueToTest);

    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            "Throw ToCQLCodecException when attempting to convert List<INT> from List<TEXT>");

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.getMessage()).contains("no codec matching (list/set)");
            });
  }

  @Test
  public void invalidVectorValueNonNumberFail() {
    DataType cqlTypeToTest = DataTypes.vectorOf(DataTypes.FLOAT, 1);
    List<JsonLiteral<?>> valueToTest = List.of(stringLiteral("abc"));
    assertToCQLFail(
        cqlTypeToTest, valueToTest, "expected JSON Number value as Vector element at position #0");
  }

  @Test
  public void invalidVectorValueWrongDimensionFail() {
    DataType cqlTypeToTest = DataTypes.vectorOf(DataTypes.FLOAT, 1);
    List<JsonLiteral<?>> valueToTest = List.of(numberLiteral(1.0), numberLiteral(-0.5));
    assertToCQLFail(
        cqlTypeToTest, valueToTest, "expected vector of length 1, got one with 2 elements");
  }

  @Test
  public void invalidVectorBadBase64Fail() {
    DataType cqlTypeToTest = DataTypes.vectorOf(DataTypes.FLOAT, 3);
    EJSONWrapper valueToTest = binaryWrapper("not-base-64");
    assertToCQLFail(
        cqlTypeToTest,
        valueToTest,
        "String not valid Base64-encoded content, problem: Illegal character");
  }

  @Test
  public void invalidVectorBase64WrongLength() {
    DataType cqlTypeToTest = DataTypes.vectorOf(DataTypes.FLOAT, 3);
    byte[] rawBase64 = CqlVectorUtil.floatsToBytes(new float[] {-0.5f, 0.25f});
    EJSONWrapper valueToTest = binaryWrapper(rawBase64);
    assertToCQLFail(
        cqlTypeToTest, valueToTest, "expected vector of length 3, got one with 2 elements");
  }

  private void assertToCQLFail(DataType cqlType, Object valueToTest, String... expectedMessages) {
    var codec = assertGetCodecToCQL(cqlType, valueToTest);

    ToCQLCodecException error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            String.format(
                "Throw ToCQLCodecException when attempting to convert `%s` from value of %s",
                cqlType, valueToTest));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(cqlType);
              assertThat(e.value).isEqualTo(valueToTest);

              for (String expectedMessage : expectedMessages) {
                assertThat(e.getMessage()).contains(expectedMessage);
              }
            });
  }
}
