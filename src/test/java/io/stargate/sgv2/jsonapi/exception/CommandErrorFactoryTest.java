package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;

import io.micrometer.core.instrument.Tag;
import io.stargate.sgv2.jsonapi.api.model.command.CommandError;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.metrics.ExceptionMetrics;
import java.util.List;
import org.junit.jupiter.api.Test;

/** tests for {@link CommandErrorFactory} */
public class CommandErrorFactoryTest extends ConfiguredErrorTest {

  private final ErrorTestData TEST_DATA = new ErrorTestData();

  @Override
  String getErrorConfigResource() {
    return TEST_DATA.TEST_ERROR_CONFIG_FILE;
  }

  @Test
  public void commandError() {
    var exception = TestRequestException.Code.NO_VARIABLES_TEMPLATE.get();
    var result = CommandErrorFactory.create(exception);
    assertCommandError(exception, result);
  }

  private void assertCommandError(APIException exception, CommandError commandError) {

    assertThat(commandError)
        .as("CommandError matches ApiException %s", exception)
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
        .contains(tuple(ExceptionMetrics.TAG_NAME_ERROR_CODE, exception.fullyQualifiedCode()));
  }
}
