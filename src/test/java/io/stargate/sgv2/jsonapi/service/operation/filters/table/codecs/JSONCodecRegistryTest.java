package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.exception.catchable.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JSONCodecRegistryTest {

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
                  .isEqualTo(fromValue.getClass());
            });
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
  @MethodSource("validCodecToCQLTestCasesOther")
  public void codecToCQLOther(DataType cqlType, Object fromValue, Object expectedCqlValue) {
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

    assertThat(actualCqlValue)
        .as(
            "Comparing expected and actual CQL value for fromValue.class=%s and fromValue.toString()=%s",
            fromValue.getClass().getName(), fromValue.toString())
        .hasSameClassAs(expectedCqlValue)
        .isEqualTo(expectedCqlValue);
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

  private static Stream<Arguments> validCodecToCQLTestCasesOther() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql
    return Stream.of(
        // Short regular base64-encoded string
        Arguments.of(
            DataTypes.BLOB,
            binaryWrapper(TEST_DATA.BASE64_PADDED_ENCODED_STR),
            ByteBuffer.wrap(TEST_DATA.BASE64_PADDED_DECODED_BYTES)),
        // edge case: empty String -> byte[0]
        Arguments.of(DataTypes.BLOB, binaryWrapper(""), ByteBuffer.wrap(new byte[0])));
  }

  private static EJSONWrapper binaryWrapper(String base64Encoded) {
    return new EJSONWrapper(
        EJSONWrapper.EJSONType.BINARY, JsonNodeFactory.instance.textNode(base64Encoded));
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
    var codec = assertGetCodecToCQL(typeToTest, valueToTest);

    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            String.format(
                "Throw ToCQLCodecException for out of range `%s` value: %s",
                typeToTest, valueToTest));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(typeToTest);
              assertThat(e.value).isEqualTo(valueToTest);

              assertThat(e.getMessage())
                  .contains(typeToTest.toString())
                  .contains(valueToTest.getClass().getName())
                  .contains(valueToTest.toString())
                  .contains("Root cause: " + rootCause);
            });
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
    var codec = assertGetCodecToCQL(typeToTest, valueToTest);

    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            String.format(
                "Throw ToCQLCodecException when attempting to convert `%s` from non-integer value %s",
                typeToTest, valueToTest));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(typeToTest);
              assertThat(e.value).isEqualTo(valueToTest);

              assertThat(e.getMessage())
                  .contains(typeToTest.toString())
                  .contains(valueToTest.getClass().getName())
                  .contains(valueToTest.toString())
                  .contains("Root cause: Rounding necessary");
            });
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
    var codec = assertGetCodecToCQL(DataTypes.ASCII, valueToTest);

    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest),
            String.format(
                "Throw ToCQLCodecException when attempting to convert `%s` from non-ASCII value %s",
                DataTypes.ASCII, valueToTest));

    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(DataTypes.ASCII);
              assertThat(e.value).isEqualTo(valueToTest);

              assertThat(e.getMessage())
                  .contains(DataTypes.ASCII.toString())
                  .contains(valueToTest.getClass().getName())
                  .contains(valueToTest.toString())
                  .contains("Root cause: String contains non-ASCII character at index");
            });
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
  public void badBinaryInputs() {
    EJSONWrapper valueToTest1 =
        new EJSONWrapper(
            EJSONWrapper.EJSONType.BINARY, JsonNodeFactory.instance.textNode("bad-base64!"));
    final var codec = assertGetCodecToCQL(DataTypes.BLOB, valueToTest1);
    var error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest1),
            "Throw ToCQLCodecException when attempting to convert DataTypes.BLOB from invalid Base64 value");
    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(DataTypes.BLOB);
              assertThat(e.value).isEqualTo(valueToTest1);
              assertThat(e.getMessage())
                  .contains("Root cause: Invalid content in EJSON $binary wrapper");
            });

    EJSONWrapper valueToTest2 =
        new EJSONWrapper(EJSONWrapper.EJSONType.BINARY, JsonNodeFactory.instance.numberNode(42));

    error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest2),
            "Throw ToCQLCodecException when attempting to convert DataTypes.BLOB from non-String EJSONWrapper value");
    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(DataTypes.BLOB);
              assertThat(e.value).isEqualTo(valueToTest2);
              assertThat(e.getMessage())
                  .contains(
                      "Root cause: Unsupported JSON value type in EJSON $binary wrapper (NUMBER): only STRING allowed");
            });

    // Test with unpadded base64
    EJSONWrapper valueToTest3 = binaryWrapper(TEST_DATA.BASE64_UNPADDED_ENCODED_STR);
    error =
        assertThrowsExactly(
            ToCQLCodecException.class,
            () -> codec.toCQL(valueToTest3),
            "Throw ToCQLCodecException when attempting to convert DataTypes.BLOB from non-String EJSONWrapper value");
    assertThat(error)
        .satisfies(
            e -> {
              assertThat(e.targetCQLType).isEqualTo(DataTypes.BLOB);
              assertThat(e.value).isEqualTo(valueToTest3);
              assertThat(e.getMessage())
                  .contains("Unexpected end of base64-encoded String")
                  .contains("expects padding");
            });
  }

  private static Stream<Arguments> nonAsciiValueFailTestCases() {
    return Stream.of(
        Arguments.of(TEST_DATA.STRING_WITH_2BYTE_UTF8_CHAR),
        Arguments.of(TEST_DATA.STRING_WITH_3BYTE_UTF8_CHAR),
        Arguments.of(TEST_DATA.STRING_WITH_4BYTE_SURROGATE_CHAR));
  }
}
