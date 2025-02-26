package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ProjectionPathTest {

  @ParameterizedTest
  @MethodSource("decodePathToSegmentsTestCases")
  public void decodePathToSegmentsTest(
      String path, List<String> expectedResult, String description) {
    List<String> segments = new ArrayList<>();
    ProjectionPath projectionPath = ProjectionPath.from(path);
    for (int i = 0; i < projectionPath.getSegmentsSize(); i++) {
      segments.add(projectionPath.getSegment(i));
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
  public <T extends APIException> void encodePathTest(
      String path, String code, Class<T> errorClass, String message, String description) {
    T error = assertThrows(errorClass, () -> ProjectionPath.from(path), description);
    assertThat(error).as(description).isInstanceOf(errorClass);
    assertThat(error.code).isEqualTo(code);
    assertThat(error.getMessage()).contains(message);
  }

  private static Stream<Arguments> invalidDecodePathToSegmentsTestCases() {
    return Stream.of(
        Arguments.of(
            ".path",
            ProjectionException.Code.UNSUPPORTED_PROJECTION_PATH.name(),
            ProjectionException.class,
            "The segments from the path in the projection cannot be empty.",
            "The path starts with `.` which will result in an empty segment, which is not allowed."),
        Arguments.of(
            "foo..bar",
            ProjectionException.Code.UNSUPPORTED_PROJECTION_PATH.name(),
            ProjectionException.class,
            "The segments from the path in the projection cannot be empty.",
            "The path contains two consecutive `.` which will result in an empty segment, which is not allowed."),
        Arguments.of(
            "path.",
            ProjectionException.Code.UNSUPPORTED_PROJECTION_PATH.name(),
            ProjectionException.class,
            "The segments from the path in the projection cannot be empty.",
            "The path ends with `.` which will result in an empty segment, which is not allowed."),
        Arguments.of(
            "path&",
            ProjectionException.Code.UNSUPPORTED_AMPERSAND_ESCAPE_USAGE.name(),
            ProjectionException.class,
            "The usage of ampersand escape is not supported.",
            "& is not followed by a valid escape character."),
        Arguments.of(
            "price&usd",
            ProjectionException.Code.UNSUPPORTED_AMPERSAND_ESCAPE_USAGE.name(),
            ProjectionException.class,
            "The usage of ampersand escape is not supported.",
            "& is not followed by a valid escape character."));
  }

  @ParameterizedTest
  @MethodSource("encodePathTestCases")
  public void encodePathTest(String path, String expectedResult, String description) {
    assertThat(ProjectionPath.encode(path)).as(description).isEqualTo(expectedResult);
  }

  private static Stream<Arguments> encodePathTestCases() {
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
}
