package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorV2;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugConfigAccess;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** tests for {@link CommandErrorFactory} */
public class APIExceptionCommandErrorBuilderTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }

  @Test
  public void productionModeCommandResult() {
    withDebugMode(
        false,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result =
              new CommandErrorFactory(true).buildLegacyCommandResultError(exception);
          assertCommandError(exception, result, 5, true);
        });
  }

  @Test
  public void productionModeCommandErrorV2() {
    withDebugMode(
        false,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result = new CommandErrorFactory(true).buildCommandErrorV2(exception);
          assertCommandErrorV2(exception, result);
        });
  }

  @Test
  public void preErrorV2ModeCommandResult() {
    withDebugMode(
        false,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result =
              new CommandErrorFactory(true).buildLegacyCommandResultError(exception);
          assertCommandError(exception, result, 1, false);
        });
  }

  @Test
  public void debugModeCommandResult() {
    withDebugMode(
        true,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result =
              new CommandErrorFactory(true).buildLegacyCommandResultError(exception);
          assertCommandError(exception, result, 6, true);

          assertThat(result)
              .isNotNull()
              .satisfies(
                  e ->
                      assertThat(e.fields())
                          .containsEntry(
                              ErrorObjectV2Constants.Fields.EXCEPTION_CLASS,
                              exception.getClass().getSimpleName()));
        });
  }

  @Test
  public void debugModeCommandErrorV2() {
    withDebugMode(
        true,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result = new CommandErrorFactory(true).buildCommandErrorV2(exception);
          assertCommandErrorV2(exception, result);

          assertThat(result)
              .isNotNull()
              .satisfies(
                  e ->
                      assertThat(e.exceptionClass())
                          .isEqualTo(exception.getClass().getSimpleName()));
        });
  }

  private void withDebugMode(boolean debugMode, Runnable runnable) {
    final boolean previousDebugMode = DebugConfigAccess.isDebugEnabled();
    DebugConfigAccess.setDebugEnabled(debugMode);
    try {
      runnable.run();
    } finally {
      DebugConfigAccess.setDebugEnabled(previousDebugMode);
    }
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
              assertMetricTags(exception, e.fieldsForMetricsTag());
              if (errorObjectV2) {
                assertErrorFields(exception, e, fieldSize, errorObjectV2);
              }
            });
  }

  private void assertCommandErrorV2(APIException exception, CommandErrorV2 commandError) {

    assertThat(commandError)
        .as("CommandErrorV2 matches ApiException %s", exception)
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.getFamily()).isEqualTo(exception.family.name());
              assertThat(e.getScope()).isEqualTo(exception.scope);
              assertThat(e.getErrorCode()).isEqualTo(exception.code);
              assertThat(e.getTitle()).isEqualTo(exception.title);
              assertThat(e.getMessage()).isEqualTo(exception.body);
              assertThat(e.getId()).isEqualTo(exception.errorId);
              assertThat(e.httpStatus().getStatusCode()).isEqualTo(exception.httpStatus);

              assertMetricTags(exception, e.metricTags());
            });
  }

  private void assertMetricTags(APIException exception, Map<String, Object> metricTags) {

    assertThat(metricTags)
        .containsEntry(ErrorObjectV2Constants.MetricTags.ERROR_CODE, exception.fullyQualifiedCode())
        .containsEntry(
            ErrorObjectV2Constants.MetricTags.EXCEPTION_CLASS,
            exception.getClass().getSimpleName());
  }

  private void assertErrorFields(
      APIException exception, CommandResult.Error error, int fieldSize, boolean errorObjectV2) {
    // always has code, only the other fields when it is the v2 schema
    assertThat(error.fields())
        .as("Error fields - common fields for v1 and v2 schema")
        .hasSize(fieldSize)
        .containsEntry(ErrorObjectV2Constants.Fields.CODE, exception.code);

    if (errorObjectV2) {
      assertThat(error.fields())
          .as("Error fields - fields new in v2 schema")
          .containsEntry(ErrorObjectV2Constants.Fields.FAMILY, exception.family.name())
          .containsEntry(ErrorObjectV2Constants.Fields.SCOPE, exception.scope)
          .containsEntry(ErrorObjectV2Constants.Fields.TITLE, exception.title)
          .containsEntry(ErrorObjectV2Constants.Fields.ID, exception.errorId);
    }
  }
}
