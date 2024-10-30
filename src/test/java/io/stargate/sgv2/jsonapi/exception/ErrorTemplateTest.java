package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Test creating the new API Exceptions using YAML templating in the file {@link
 * ErrorTestData#TEST_ERROR_CONFIG_FILE}
 */
public class ErrorTemplateTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }

  /**
   * Stnadard way to create an exception from template, that expects the name and value params
   *
   * @param errorCode
   * @return
   * @param <T>
   */
  private <T extends APIException> T createException(ErrorCode<T> errorCode) {

    T error =
        assertDoesNotThrow(
            () ->
                errorCode.get(
                    Map.of(
                        "name", TEST_DATA.VAR_NAME,
                        "value", TEST_DATA.VAR_VALUE)),
            String.format("Creating exception with template %s", errorCode.template()));

    // basic test that the name and value got put into the body
    assertThat(error)
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.body).contains(TEST_DATA.VAR_NAME);
              assertThat(e.body).contains(TEST_DATA.VAR_VALUE);
            });
    return error;
  }

  /** Re-usable to Test various properties on an error */
  private <T extends APIException> void assertError(
      T error,
      ErrorFamily family,
      String scope,
      String code,
      String title,
      Integer httpStatusOverride) {
    // Does not accept a template because that would not catch the template being wrong
    // pass in the values that error should have given the code etc.

    assertThat(error)
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.family).isEqualTo(family);
              assertThat(e.scope).isEqualTo(scope);
              assertThat(e.code).isEqualTo(code);
              assertThat(e.title).isEqualTo(title);
              if (null == httpStatusOverride) {
                assertThat(e.httpStatus).isEqualTo(APIException.DEFAULT_HTTP_STATUS);
              } else {
                assertThat(e.httpStatus).isEqualTo(httpStatusOverride);
              }
            });
  }

  @Test
  public void scopedRequestError() {

    var error = createException(TestScopeException.Code.SCOPED_REQUEST_ERROR);
    assertThat(error)
        .isNotNull()
        .satisfies(
            e -> {
              assertError(
                  e,
                  TestScopeException.FAMILY,
                  TestScopeException.SCOPE.name(),
                  TestScopeException.Code.SCOPED_REQUEST_ERROR.name(),
                  "A scoped request error",
                  null);
              assertThat(e.body)
                  .contains(
                      "long body with "
                          + TEST_DATA.VAR_NAME
                          + " and "
                          + TEST_DATA.VAR_VALUE
                          + " in it");
            });
  }

  @Test
  public void unscopedRequestError() {

    var error = createException(TestRequestException.Code.UNSCOPED_REQUEST_ERROR);
    assertThat(error)
        .isNotNull()
        .satisfies(
            e -> {
              assertError(
                  e,
                  TestRequestException.FAMILY,
                  "",
                  TestRequestException.Code.UNSCOPED_REQUEST_ERROR.name(),
                  "An unscoped request error",
                  null);
              assertThat(e.body)
                  .contains(
                      "Multi line with "
                          + TEST_DATA.VAR_NAME
                          + " and "
                          + TEST_DATA.VAR_VALUE
                          + " in it.\n"
                          + "And a snippet below:\n"
                          + "Please contact support if the issue persists.");
            });
  }

  @Test
  public void missingTemplateVars() {

    var error =
        assertThrows(
            UnresolvedErrorTemplateVariable.class,
            () -> TestScopeException.Code.SCOPED_REQUEST_ERROR.get("name", TEST_DATA.VAR_NAME),
            "Missing Vars");

    assertThat(error)
        .as("Error message is for the missing var")
        .message()
        .isEqualTo(
            "Unresolved variable in error template for family: REQUEST, scope: TEST_REQUEST_SCOPE, code: SCOPED_REQUEST_ERROR, keys: [name, SNIPPET.CONTACT_SUPPORT] error: Cannot resolve variable 'value' (enableSubstitutionInVariables=false).");

    assertThat(error.errorTemplate)
        .as("Error template is the one we triggered")
        .isNotNull()
        .isEqualTo(TestScopeException.Code.SCOPED_REQUEST_ERROR.template());
  }

  @Test
  public void apiExceptionMessageIsBody() {

    var error = createException(TestRequestException.Code.UNSCOPED_REQUEST_ERROR);
    assertThat(error)
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.getMessage()).isEqualTo(e.body);
            });
  }

  @Test
  public void apiExceptionToString() {

    var error = createException(TestRequestException.Code.UNSCOPED_REQUEST_ERROR);

    assertThat(error)
        .describedAs("toString should contain all the fields")
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.toString()).contains(e.getClass().getSimpleName());
              assertThat(e.toString()).contains(e.family.name());
              assertThat(e.toString()).contains(e.scope);
              assertThat(e.toString()).contains(e.code);
              assertThat(e.toString()).contains(e.title);
              assertThat(e.toString()).contains(e.body);
            });
  }

  @Test
  public void unknownErrorCode() {

    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ErrorTemplate.load(
                    TestScopeException.class,
                    TestScopeException.FAMILY,
                    TestScopeException.SCOPE,
                    TEST_DATA.RANDOM_STRING),
            "Error code in enum but not in yaml");

    assertThat(error)
        .message()
        .startsWith(
            String.format(
                "Could not find error config for family=%s, scope=%s, code=%s",
                TestScopeException.FAMILY, TestScopeException.SCOPE, TEST_DATA.RANDOM_STRING));
  }

  @Test
  public void httpStatusOverride() {
    var error = createException(TestRequestException.Code.HTTP_OVERRIDE);

    assertThat(error)
        .isNotNull()
        .satisfies(
            e -> {
              assertError(
                  e,
                  TestRequestException.FAMILY,
                  "",
                  TestRequestException.Code.HTTP_OVERRIDE.name(),
                  "An error that overrides the HTTP response",
                  500);
            });
  }
}
