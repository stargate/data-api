package io.stargate.sgv2.jsonapi.exception;

import static io.stargate.sgv2.jsonapi.exception.ExceptionFlags.UNRELIABLE_DB_SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.EnumSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Testing the methods on the {@link ErrorCode} interface, not testing the actual error codes.
 *
 * <p>Uses config in {@link ErrorTestData#TEST_ERROR_CONFIG_FILE}
 */
public class ErrorCodeTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }

  @Test
  public void allGetWithVarsReturnSame() {
    var errorCode = TestScopeException.Code.SCOPED_REQUEST_ERROR;

    var errorFromParamArgs =
        assertDoesNotThrow(
            () ->
                errorCode.get(
                    "name", TEST_DATA.VAR_NAME,
                    "value", TEST_DATA.VAR_VALUE));

    var errorFromMap =
        assertDoesNotThrow(
            () ->
                errorCode.get(
                    Map.of(
                        "name", TEST_DATA.VAR_NAME,
                        "value", TEST_DATA.VAR_VALUE)),
            String.format("Creating exception with template %s", errorCode.template()));

    assertThat(errorFromParamArgs)
        .describedAs("Error from param args and error from map are same ignoring errorId")
        .usingRecursiveComparison()
        .ignoringFields("errorId")
        .isEqualTo(errorFromMap);

    assertThat(errorFromParamArgs.errorId)
        .describedAs("errorId is different between from param args and map")
        .isNotEqualTo(errorFromMap.errorId);
  }

  @Test
  public void getWithExceptionActions() {
    var errorCode = TestScopeException.Code.SCOPED_REQUEST_ERROR;
    var actions = EnumSet.of(UNRELIABLE_DB_SESSION);

    var errorFromParamArgs =
        assertDoesNotThrow(
            () -> errorCode.get(actions, "name", TEST_DATA.VAR_NAME, "value", TEST_DATA.VAR_VALUE));

    var errorFromMap =
        assertDoesNotThrow(
            () ->
                errorCode.get(
                    actions,
                    Map.of(
                        "name", TEST_DATA.VAR_NAME,
                        "value", TEST_DATA.VAR_VALUE)),
            String.format("Creating exception with template %s", errorCode.template()));

    assertThat(errorFromParamArgs.exceptionFlags)
        .as("Exception created with param args carries the given actions")
        .isEqualTo(actions);
    assertThat(errorFromMap.exceptionFlags)
        .as("Exception created with map carries the given actions")
        .isEqualTo(actions);
  }

  @Test
  public void getNoVars() {
    var errorCode = TestRequestException.Code.NO_VARIABLES_TEMPLATE;

    assertDoesNotThrow(
        () -> errorCode.get(), "Can create error that does not have vars in the template ");
  }
}
