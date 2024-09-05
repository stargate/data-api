package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import org.junit.jupiter.api.Test;

/** tests for {@link APIExceptionCommandErrorBuilder} */
public class APIExceptionCommandErrorBuilderTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }

  @Test
  public void productionModeCommandResult() {
    var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
    var result = new APIExceptionCommandErrorBuilder(false, true).apply(exception);
    assertCommandError(exception, result, 4, true);
  }

  @Test
  public void preErrorV2ModeCommandResult() {
    var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
    var result = new APIExceptionCommandErrorBuilder(false, true).apply(exception);
    assertCommandError(exception, result, 1, false);
  }

  @Test
  public void debugModeCommandResult() {
    var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
    var result = new APIExceptionCommandErrorBuilder(true, true).apply(exception);
    assertCommandError(exception, result, 5, true);

    assertThat(result)
        .isNotNull()
        .satisfies(
            e ->
                assertThat(e.fields())
                    .containsEntry(
                        ErrorObjectV2Constants.Fields.EXCEPTION_CLASS,
                        exception.getClass().getSimpleName()));
  }

  private void assertCommandError(
      APIException exception,
      CommandResult.Error commandError,
      int fieldSize,
      boolean errorObjectV2) {

    assertThat(commandError)
        .as("CommandResult.Error matches ApiException %s", exception)
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.message()).isEqualTo(exception.body);
              assertThat(e.httpStatus().getStatusCode()).isEqualTo(exception.httpStatus);
              assertMetricTags(exception, e);
              if (errorObjectV2) {
                assertErrorFields(exception, e, fieldSize, errorObjectV2);
              }
            });
  }

  private void assertMetricTags(APIException exception, CommandResult.Error error) {

    assertThat(error.fieldsForMetricsTag())
        .containsEntry(ErrorObjectV2Constants.MetricTags.ERROR_CODE, exception.fullyQualifiedCode())
        .containsEntry(
            ErrorObjectV2Constants.MetricTags.EXCEPTION_CLASS,
            exception.getClass().getSimpleName());
  }

  private void assertErrorFields(
      APIException exception, CommandResult.Error error, int fieldSize, boolean errorObjectV2) {
    // always has code, only te othes when it is the v2 schema
    assertThat(error.fields())
        .as("Error fields - common fields for v1 and v2 schema")
        .hasSize(fieldSize)
        .containsEntry(ErrorObjectV2Constants.Fields.CODE, exception.code);

    if (errorObjectV2) {
      assertThat(error.fields())
          .as("Error fields - fields new in v2 schema")
          .containsEntry(ErrorObjectV2Constants.Fields.FAMILY, exception.family.name())
          .containsEntry(ErrorObjectV2Constants.Fields.SCOPE, exception.scope)
          .containsEntry(ErrorObjectV2Constants.Fields.TITLE, exception.title);
    }
  }
}
