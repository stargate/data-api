package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.math.BigDecimal;
import java.math.BigInteger;
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
                JSONCodecRegistry.codecToCQL(
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
  @MethodSource("codecToCQLTestCases")
  public void codecToCQL(DataType cqlType, Object fromValue, Object expectedCqlValue) {

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

  private static Stream<Arguments> codecToCQLTestCases() {
    // Arguments: (CQL-type, from-caller, bound-by-driver-for-cql
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
            DataTypes.VARINT, BigDecimal.valueOf(123456789.0), BigInteger.valueOf(123456789)),

        // Floating-point types:
        Arguments.of(DataTypes.DECIMAL, BigDecimal.valueOf(0.25), BigDecimal.valueOf(0.25)));
  }

  @Test
  public void missingJSONCodecException() {

    var error =
        assertThrowsExactly(
            MissingJSONCodecException.class,
            () ->
                JSONCodecRegistry.codecToCQL(
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
                JSONCodecRegistry.codecToCQL(
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
                "Throw ToCQLCodecException for out of range `%s` %s", typeToTest, valueToTest));

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
        // Arguments.of(DataTypes.BIGINT, TEST_DATA.OUT_OF_RANGE_FOR_BIGINT.longValueExact(),
        // "Overflow"),
        Arguments.of(DataTypes.INT, TEST_DATA.OUT_OF_RANGE_FOR_INT, "Overflow"),
        Arguments.of(
            DataTypes.INT,
            TEST_DATA.OUT_OF_RANGE_FOR_INT.toBigIntegerExact(),
            "BigInteger out of int range"),
        // Arguments.of(DataTypes.INT, TEST_DATA.OUT_OF_RANGE_FOR_INT.longValueExact(), "Overflow"),
        Arguments.of(DataTypes.SMALLINT, TEST_DATA.OUT_OF_RANGE_FOR_SMALLINT, "Overflow"),
        Arguments.of(
            DataTypes.SMALLINT,
            TEST_DATA.OUT_OF_RANGE_FOR_SMALLINT.toBigIntegerExact(),
            "BigInteger out of short range"),
        // Arguments.of(DataTypes.SMALLINT, TEST_DATA.OUT_OF_RANGE_FOR_SMALL_INT.longValueExact(),
        // "Overflow"),
        Arguments.of(DataTypes.TINYINT, TEST_DATA.OUT_OF_RANGE_FOR_TINYINT, "Overflow"),
        Arguments.of(
            DataTypes.TINYINT,
            TEST_DATA.OUT_OF_RANGE_FOR_TINYINT.toBigIntegerExact(),
            "BigInteger out of byte range")
        // Arguments.of(DataTypes.TINYINT, TEST_DATA.OUT_OF_RANGE_FOR_TINY_INT.longValueExact()),
        // "Overflow");
        );
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
}
