package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class JsonApiExceptionTest {

  @Nested
  class Get {

    @Test
    public void happyPath() {
      JsonApiException ex = new JsonApiException(ErrorCode.COMMAND_UNKNOWN);

      CommandResult result = ex.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("Provided command unknown");
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "COMMAND_UNKNOWN")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }

    @Test
    public void withCustomMessage() {
      JsonApiException ex =
          new JsonApiException(ErrorCode.COMMAND_UNKNOWN, "Custom message is more important.");

      CommandResult result = ex.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("Custom message is more important.");
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "COMMAND_UNKNOWN")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }

    @Test
    public void withCause() {
      Exception cause = new IllegalArgumentException("Cause message is important");
      JsonApiException ex = new JsonApiException(ErrorCode.COMMAND_UNKNOWN, cause);

      CommandResult result = ex.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .hasSize(2)
          .anySatisfy(
              error -> {
                assertThat(error.message()).isEqualTo("Provided command unknown");
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "COMMAND_UNKNOWN")
                    .containsEntry("exceptionClass", "JsonApiException");
              })
          .anySatisfy(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (java.lang.IllegalArgumentException) Cause message is important");
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }
  }
}
