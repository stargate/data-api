package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;

/**
 * Tests on features of the {@link APIException} class itself, most of the other tests are about
 * creating them via the templating etc.
 */
public class APIExceptionTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }

  @Test
  public void fullyQualifiedCodeNoScope() {
    var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();

    assertThat(exception.fullyQualifiedCode())
        .isEqualTo(
            String.format(
                "%s_%s",
                TestRequestException.FAMILY,
                TestRequestException.Code.NO_VARIABLES_TEMPLATE.name()));
  }

  @Test
  public void fullyQualifiedCodeWithScope() {
    var exception =
        TestScopeException.Code.SCOPED_REQUEST_ERROR.get(
            "name", TEST_DATA.VAR_NAME,
            "value", TEST_DATA.VAR_VALUE);

    assertThat(exception.fullyQualifiedCode())
        .isEqualTo(
            String.format(
                "%s_%s_%s",
                TestScopeException.FAMILY,
                TestScopeException.SCOPE.scope(),
                TestScopeException.Code.SCOPED_REQUEST_ERROR.name()));
  }

  @Test
  public void messageSameAsBody() {
    var exception =
        TestScopeException.Code.SCOPED_REQUEST_ERROR.get(
            "name", TEST_DATA.VAR_NAME,
            "value", TEST_DATA.VAR_VALUE);

    assertThat(exception).message().isEqualTo(exception.body);
  }

  @Test
  public void toStringFormatted() {
    var exception =
        TestScopeException.Code.SCOPED_REQUEST_ERROR.get(
            "name", TEST_DATA.VAR_NAME,
            "value", TEST_DATA.VAR_VALUE);

    assertThat(exception.toString())
        .as("APIException.toString() has expected properties")
        .contains(exception.getClass().getSimpleName())
        .contains(exception.family.name())
        .contains(exception.scope)
        .contains(exception.code)
        .contains(exception.title)
        .contains(exception.body);
  }

  @Test
  public void defaultXxceptionActionsFieldEmpty() {
    var exception =
        TestScopeException.Code.SCOPED_REQUEST_ERROR.get(
            "name", TEST_DATA.VAR_NAME,
            "value", TEST_DATA.VAR_VALUE);

    assertThat(exception.exceptionActions)
        .as("exceptionActions field should not be null and should be empty EnumSet")
        .isNotNull()
        .isEmpty();
  }

  @Test
  public void exceptionActionsSetViaConstructor() {
    var exception =
        TestScopeException.Code.SCOPED_REQUEST_ERROR.get(
            EnumSet.of(ExceptionAction.EVICT_SESSION_CACHE),
            "name",
            TEST_DATA.VAR_NAME,
            "value",
            TEST_DATA.VAR_VALUE);

    assertThat(exception.exceptionActions)
        .as("exceptionActions should contain EVICT_SESSION_CACHE")
        .contains(ExceptionAction.EVICT_SESSION_CACHE);
  }
}
