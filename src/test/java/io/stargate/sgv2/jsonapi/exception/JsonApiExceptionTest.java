package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.util.ClassUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class JsonApiExceptionTest {

  @Test
  public void happyPath() {

    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addThrowable(new JsonApiException(ErrorCodeV1.INVALID_REQUEST))
            .build();

    assertThat(commandResult.data()).isNull();
    assertThat(commandResult.status()).isEmpty();
    assertThat(commandResult.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.message()).isEqualTo("Request not supported by the data store");
              assertThat(error.errorCode()).isEqualTo(ErrorCodeV1.INVALID_REQUEST.name());
              assertThat(error.errorClass())
                  .isEqualTo(ClassUtils.classSimpleName(JsonApiException.class));
            });
  }
}
