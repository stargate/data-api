package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;

import io.micrometer.core.instrument.Tag;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorV2;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugConfigAccess;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;

import java.util.List;
import java.util.Map;

import io.stargate.sgv2.jsonapi.metrics.ExceptionMetrics;
import org.junit.jupiter.api.Test;

/** tests for {@link CommandErrorFactory} */
public class APIExceptionCommandErrorBuilderTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }


  @Test
  public void productionModeCommandErrorV2() {
    withDebugMode(
        false,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result = new CommandErrorFactory().create(exception);
          assertCommandErrorV2(exception, result);
        });
  }


  @Test
  public void debugModeCommandErrorV2() {
    withDebugMode(
        true,
        () -> {
          var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
          var result = new CommandErrorFactory().create(exception);
          assertCommandErrorV2(exception, result);

          assertThat(result)
              .isNotNull()
              .satisfies(
                  e ->
                      assertThat(e.errorClass())
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

  private void assertCommandErrorV2(APIException exception, CommandErrorV2 commandError) {

    assertThat(commandError)
        .as("CommandErrorV2 matches ApiException %s", exception)
        .isNotNull()
        .satisfies(
            e -> {
              assertThat(e.family()).isEqualTo(exception.family.name());
              assertThat(e.scope()).isEqualTo(exception.scope);
              assertThat(e.errorCode()).isEqualTo(exception.code);
              assertThat(e.title()).isEqualTo(exception.title);
              assertThat(e.message()).isEqualTo(exception.body);
              assertThat(e.id()).isEqualTo(exception.errorId);
              assertThat(e.httpStatus().getStatusCode()).isEqualTo(exception.httpStatus);

              assertMetricTags(exception, e.metricTags());
            });
  }

  private void assertMetricTags(APIException exception, List<Tag> metricTags) {

    assertThat(metricTags)
        .extracting(Tag::getKey, Tag::getValue)
        .contains(
            tuple(
                ExceptionMetrics.TAG_NAME_ERROR_CODE,
                exception.fullyQualifiedCode()),
            tuple(
                ExceptionMetrics.TAG_NAME_ERROR_CLASS,
                exception.getClass().getSimpleName())
        );
  }
}
