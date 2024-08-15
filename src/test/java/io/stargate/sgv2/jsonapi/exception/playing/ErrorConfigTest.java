package io.stargate.sgv2.jsonapi.exception.playing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileNotFoundException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ErrorConfigTest {

  @Test
  public void initializeFromResource() throws JsonProcessingException {

    // test that we load from a resource, and that it can only happen once
    // we will assume other tests that just look for an exception from normal code will cause the
    // autoloading to happen
    // Have to use Unsafe first because cannot guarantee the order tests are run, another test could
    // have set it
    assertDoesNotThrow(
        () -> ErrorConfig.unsafeInitializeFromYamlResource(ErrorConfig.DEFAULT_ERROR_CONFIG_FILE),
        "Can load the error config from yaml resource file");

    assertThrows(
        IllegalStateException.class,
        () -> ErrorConfig.initializeFromYamlResource(ErrorConfig.DEFAULT_ERROR_CONFIG_FILE),
        "Can only load the error config from yaml resource file once");

    assertDoesNotThrow(
        ErrorConfig::getInstance,
        "Can get the error config instance after loading from yaml resource file");
  }

  @Test
  public void readErrorsYaml() throws JsonProcessingException {
    String yaml =
        """
        request_errors:
          - scope: TEST_SCOPE_1
            code: TEST_ERROR_ID_1
            title: the title for the error
            body: |-
              big long body with ${vars} in it
          - scope:
            code: TEST_ERROR_ID_2
            title: This error has no scope
            body: |-
              Line 1 of body

              Line 2 of body
        server_errors:
          - scope: TEST_SCOPE_3
            code: TEST_ERROR_ID_2
            title: the title for the error
            body: big long body with ${vars} in it""";

    var errorConfig = ErrorConfig.readFromYamlString(yaml);

    assertThat(errorConfig.getErrorDetail(ErrorFamily.REQUEST, "TEST_SCOPE_1", "TEST_ERROR_ID_1"))
        .isPresent()
        .get()
        .satisfies(
            e -> {
              assertThat(e.scope()).isEqualTo("TEST_SCOPE_1");
              assertThat(e.code()).isEqualTo("TEST_ERROR_ID_1");
              assertThat(e.title()).isEqualTo("the title for the error");
              assertThat(e.body()).isEqualTo("big long body with ${vars} in it");
            });

    assertThat(errorConfig.getErrorDetail(ErrorFamily.REQUEST, "", "TEST_ERROR_ID_2"))
        .isPresent()
        .get()
        .satisfies(
            e -> {
              assertThat(e.scope()).isEmpty();
              assertThat(e.code()).isEqualTo("TEST_ERROR_ID_2");
              assertThat(e.title()).isEqualTo("This error has no scope");
              assertThat(e.body()).isEqualTo("Line 1 of body\n\nLine 2 of body");
            });

    assertThat(errorConfig.getErrorDetail(ErrorFamily.SERVER, "TEST_SCOPE_3", "TEST_ERROR_ID_2"))
        .isPresent()
        .get()
        .satisfies(
            e -> {
              assertThat(e.scope()).isEqualTo("TEST_SCOPE_3");
              assertThat(e.code()).isEqualTo("TEST_ERROR_ID_2");
              assertThat(e.title()).isEqualTo("the title for the error");
              assertThat(e.body()).isEqualTo("big long body with ${vars} in it");
            });
  }

  @ParameterizedTest
  @MethodSource("errorDetailTestCases")
  public <T extends Throwable> void errorDetailValidation(
      String scope, String code, String title, String body, Class<T> errorClass, String message) {

    T error =
        assertThrows(
            errorClass, () -> new ErrorConfig.ErrorDetail(scope, code, title, body), "Error Type");

    assertThat(error.getMessage()).as("Error Message").isEqualTo(message);
  }

  private static Stream<Arguments> errorDetailTestCases() {
    return Stream.of(
        Arguments.of(
            "not_snake_scope",
            "CODE",
            "title",
            "body",
            IllegalArgumentException.class,
            "scope must be in UPPER_SNAKE_CASE_1 format, got: not_snake_scope"),
        Arguments.of(
            "SCOPE", null, "title", "body", NullPointerException.class, "code cannot be null"),
        Arguments.of(
            "SCOPE",
            "not snake code",
            "title",
            "body",
            IllegalArgumentException.class,
            "code must be in UPPER_SNAKE_CASE_1 format, got: not snake code"),
        Arguments.of(
            "SCOPE", "CODE", null, "body", NullPointerException.class, "title cannot be null"),
        Arguments.of(
            "SCOPE", "CODE", "", "body", IllegalArgumentException.class, "title cannot be blank"),
        Arguments.of(
            "SCOPE", "CODE", "title", null, NullPointerException.class, "body cannot be null"),
        Arguments.of(
            "SCOPE", "CODE", "title", "", IllegalArgumentException.class, "body cannot be blank"));
  }

  @Test
  public void readSnippetYaml() throws JsonProcessingException {
    String yaml =
        """
        snippets:
          - name: SNIPPET_1
            body: |-
              Snippet 1 body
          - name: SNIPPET_2
            body: |-
              Snippet 2 body

              multi line
            """;

    var errorConfig = ErrorConfig.readFromYamlString(yaml);

    assertThat(
            errorConfig.snippets().stream().filter(e -> e.name().equals("SNIPPET_1")).findFirst())
        .isPresent()
        .get()
        .satisfies(
            s -> {
              assertThat(s.name()).isEqualTo("SNIPPET_1");
              assertThat(s.body()).isEqualTo("Snippet 1 body");
            });
    assertThat(
            errorConfig.snippets().stream().filter(e -> e.name().equals("SNIPPET_2")).findFirst())
        .isPresent()
        .get()
        .satisfies(
            s -> {
              assertThat(s.name()).isEqualTo("SNIPPET_2");
              assertThat(s.body()).isEqualTo("Snippet 2 body\n\nmulti line");
            });
  }

  @ParameterizedTest
  @MethodSource("snippetValidationTestCases")
  public <T extends Throwable> void snippetValidation(
      String name, String body, Class<T> errorClass, String message) {

    T error = assertThrows(errorClass, () -> new ErrorConfig.Snippet(name, body), "Error Type");
    assertEquals(message, error.getMessage(), "Error Message");
  }

  private static Stream<Arguments> snippetValidationTestCases() {
    return Stream.of(
        Arguments.of(null, "body", NullPointerException.class, "name cannot be null"),
        Arguments.of(
            "not_snake_name",
            "body",
            IllegalArgumentException.class,
            "name must be in UPPER_SNAKE_CASE_1 format, got: not_snake_name"),
        Arguments.of("NAME", null, NullPointerException.class, "body cannot be null"),
        Arguments.of("NAME", "", IllegalArgumentException.class, "body cannot be blank"));
  }

  @Test
  public void snippetsVarsAreImmutable() {

    // do not care where the config comes from
    var snippetVars = ErrorConfig.getInstance().getSnippetVars();

    assertThrows(UnsupportedOperationException.class, () -> snippetVars.put("new", "value"));
    assertThrows(UnsupportedOperationException.class, snippetVars::clear);
  }

  @Test
  public void missingConfigFile() {
    assertThrows(
        FileNotFoundException.class,
        () -> ErrorConfig.unsafeInitializeFromYamlResource("missing.yaml"),
        "Error when loading a missing config file");
  }
}
