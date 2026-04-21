package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DocumentPathTest {

  @ParameterizedTest
  @MethodSource("decodePathToSegmentsTestCases")
  public void decodePathToSegmentsTest(
      String path, List<String> expectedResult, String description) {
    List<String> segments = new ArrayList<>();
    DocumentPath documentPath = DocumentPath.from(path);
    for (int i = 0; i < documentPath.getSegmentsSize(); i++) {
      segments.add(documentPath.getSegment(i));
    }
    assertThat(segments).as(description).isEqualTo(expectedResult);
  }

  private static Stream<Arguments> decodePathToSegmentsTestCases() {
    return Stream.of(
        Arguments.of("pricing.price.usd", List.of("pricing", "price", "usd"), "no escape"),
        Arguments.of("pricing.price&.usd", List.of("pricing", "price.usd"), "escape single dot"),
        Arguments.of(
            "pricing.price&.usd&.value.unit&.million",
            List.of("pricing", "price.usd.value", "unit.million"),
            "escape multiple dots"),
        Arguments.of(
            "pricing.price&&jpy", List.of("pricing", "price&jpy"), "escape single ampersand"),
        Arguments.of(
            "pricing.price&&usd&&value.unit&&million",
            List.of("pricing", "price&usd&value", "unit&million"),
            "escape multiple ampersands"),
        Arguments.of(
            "pricing.price&&&.aud", List.of("pricing", "price&.aud"), "escape dot and ampersand"),
        Arguments.of(
            "pricing.price&.usd&&value.unit&.&&million",
            List.of("pricing", "price.usd&value", "unit.&million"),
            "escape multiple dots and ampersands"),
        Arguments.of("pricing. ", List.of("pricing", " "), "black string is a valid segment"),
        Arguments.of(" ", List.of(" "), "black string is a valid segment"));
  }

  @ParameterizedTest
  @MethodSource("invalidDecodePathToSegmentsTestCases")
  public <T extends Exception> void encodeSegmentPathTest(
      String path, Class<T> errorClass, String message, String description) {
    T error = assertThrows(errorClass, () -> DocumentPath.from(path), description);
    assertThat(error).as(description).isInstanceOf(errorClass);
    assertThat(error.getMessage()).contains(message);
  }

  private static Stream<Arguments> invalidDecodePathToSegmentsTestCases() {
    return Stream.of(
        Arguments.of(
            ".path",
            IllegalArgumentException.class,
            "The path cannot contain an empty segment.",
            "The path starts with `.` which will result in an empty segment, which is not allowed."),
        Arguments.of(
            "foo..bar",
            IllegalArgumentException.class,
            "The path cannot contain an empty segment.",
            "The path contains two consecutive `.` which will result in an empty segment, which is not allowed."),
        Arguments.of(
            "path.",
            IllegalArgumentException.class,
            "Path cannot end with a dot. Each segment must be a non-empty segment.",
            "The path ends with `.` which will result in an empty segment, which is not allowed."),
        Arguments.of(
            "path&",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 4 must be followed by either '&' or '.'",
            "& is not followed by a valid escape character."),
        Arguments.of(
            "price&usd",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 5 must be followed by either '&' or '.'",
            "& is not followed by a valid escape character."));
  }

  @ParameterizedTest
  @MethodSource("encodeSegmentPathTestCases")
  public void encodeSegmentPathTest(String path, String expectedResult, String description) {
    assertThat(DocumentPath.encodeSegment(path)).as(description).isEqualTo(expectedResult);
  }

  private static Stream<Arguments> encodeSegmentPathTestCases() {
    return Stream.of(
        Arguments.of("price.usd", "price&.usd", "escape single dot"),
        Arguments.of("price&usd", "price&&usd", "escape single ampersand"),
        Arguments.of("price&.usd", "price&&&.usd", "escape dot and ampersand"),
        Arguments.of("price..value", "price&.&.value", "escape multiple dots"),
        Arguments.of("price&&value", "price&&&&value", "escape multiple ampersands"),
        Arguments.of(
            "price&.value&&.unit",
            "price&&&.value&&&&&.unit",
            "escape multiple dots and ampersands"));
  }

  @ParameterizedTest
  @MethodSource("invalidEncodedPathTestCases")
  public <T extends Exception> void invalidEncodeSegmentPathTest(
      String path, Class<T> errorClass, String message, String description) {
    T error = assertThrows(errorClass, () -> DocumentPath.verifyEncodedPath(path), description);
    assertThat(error.getMessage()).contains(message);
  }

  private static Stream<Arguments> invalidEncodedPathTestCases() {
    return Stream.of(
        Arguments.of(
            "price&.usd&",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 10 must be followed by either '&' or '.'",
            "The encoded path ends with `&` which is not allowed."),
        Arguments.of(
            "&price",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 0 must be followed by either '&' or '.'",
            "The encoded path starts with `&` which is not allowed."),
        Arguments.of(
            "price&usd",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 5 must be followed by either '&' or '.'",
            "The encoded path contains a lone `&` which is not allowed."),
        Arguments.of(
            "price&.usd&&&",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 12 must be followed by either '&' or '.'",
            "The encoded path contains a lone `&` at the end which is not allowed."),
        Arguments.of(
            "price&\tusd",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 5 must be followed by either '&' or '.'",
            "The encoded path contains an ampersand followed by a tab character which is not allowed."),
        Arguments.of(
            "price& usd",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 5 must be followed by either '&' or '.'",
            "The encoded path contains an ampersand followed by a space which is not allowed."),
        Arguments.of(
            "price&usd&value&weight",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 5 must be followed by either '&' or '.'",
            "The encoded path contains multiple lone ampersands which are not allowed."),
        Arguments.of(
            "p&rice&.usd",
            IllegalArgumentException.class,
            "The ampersand character '&' at position 1 must be followed by either '&' or '.'",
            "The encoded path contains a lone ampersand at the beginning of a segment."));
  }

  @ParameterizedTest
  @MethodSource("validEncodedPathTestCases")
  public void validEncodeSegmentPathTest(String path, String description) {
    // The method should return the path unchanged for valid paths
    Assertions.assertEquals(path, DocumentPath.verifyEncodedPath(path), description);
  }

  private static Stream<Arguments> validEncodedPathTestCases() {
    return Stream.of(
        Arguments.of("pricing.price.usd", "Simple path with no escape characters"),
        Arguments.of("r&&d", "Path with properly escaped ampersand (representing 'r&d')"),
        Arguments.of(
            "item&.price",
            "Path with properly escaped dot (representing 'item.price' as a single segment)"),
        Arguments.of(
            "product.name&&value.price&.per&.unit",
            "Complex path with properly escaped ampersands and dots"),
        Arguments.of("a&&.b&.c&&d", "Path with consecutive escape sequences"),
        Arguments.of("&&&&&&", "Path consisting only of escaped ampersands (representing '&&&')"),
        Arguments.of("&.&.&.", "Path consisting only of escaped dots"),
        Arguments.of("a", "Single segment path with no escape characters"),
        Arguments.of("a&&", "Path ending with a properly escaped ampersand"));
  }
}
