package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CqlDurationConverterTest {
  @ParameterizedTest
  @MethodSource("validCqlDurationToISO8601Duration")
  void testCqlToISO8601Duration(String input, String expectedISO8601) {
    final CqlDuration cqlDuration =
        assertDoesNotThrow(
            () -> CqlDuration.from(input),
            String.format("Converting from String '%s' to CqlDuration", input));
    String iso8601String =
        assertDoesNotThrow(
            () -> CqlDurationConverter.toISO8601Duration(cqlDuration),
            String.format(
                "Converting from '%s' (source '%s') to ISO-8601 String'", cqlDuration, input));
    assertThat(iso8601String)
        .as("Converting from '%s' to ISO-8601 String, expecting '%s'", input, expectedISO8601)
        .isEqualTo(expectedISO8601);
  }

  private static Stream<Arguments> validCqlDurationToISO8601Duration() {
    // Arguments: (input-to-cql-duration, assertions-output)
    return Stream.of(
        // Identity
        Arguments.of("P1Y2M3DT4H5M6S", "P1Y2M3DT4H5M6S"),

        // Actual conversions; manual examples:
        Arguments.of("1h30m", "PT1H30M"),
        Arguments.of("3h7s", "PT3H7S"),
        Arguments.of("22h1ms", "PT22H0.001S"),
        Arguments.of("1m37us", "PT1M0.000037S"),

        // Actual conversions; inspired by ChatGPT:
        Arguments.of("15mo10d", "P1Y3M10D"),
        Arguments.of("0mo5d1h", "P5DT1H"),
        Arguments.of("5d3h", "P5DT3H"), // alt
        Arguments.of("0mo0d1m30s61ms", "PT1M30.061S"),
        Arguments.of("24mo", "P2Y"),
        Arguments.of("25mo", "P2Y1M"),
        Arguments.of("6mo15d0s123ms456us789ns", "P6M15DT0.123456789S"),
        Arguments.of("1d23h", "P1DT23H"),
        Arguments.of("1d24h", "P1DT24H"), // alt -- interesting variation
        Arguments.of("0mo0d1s", "PT1S"),
        Arguments.of("0d1s", "PT1S"), // alt
        Arguments.of("1s", "PT1S"), // alt
        Arguments.of("3mo20d1h23m20s", "P3M20DT1H23M20S"),
        Arguments.of("0mo0d0s500ms", "PT0.5S"),
        Arguments.of("0d0s500ms", "PT0.5S"), // alt
        Arguments.of("0s500ms", "PT0.5S"), // alt
        Arguments.of("500ms", "PT0.5S"), // alt
        Arguments.of("12mo30d", "P1Y30D"));
  }
}
