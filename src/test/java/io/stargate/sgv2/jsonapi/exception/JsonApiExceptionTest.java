package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JsonApiExceptionTest {

  @Nested
  class Get {

    @Test
    public void happyPath() {
      JsonApiException ex = new JsonApiException(ErrorCode.COMMAND_NOT_IMPLEMENTED);

      CommandResult result = ex.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("The provided command is not implemented.");
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("errorCode", "COMMAND_NOT_IMPLEMENTED");
              });
    }

    @Test
    public void withCustomMessage() {
      JsonApiException ex =
          new JsonApiException(ErrorCode.COMMAND_NOT_IMPLEMENTED, "Custom message is more important.");

      CommandResult result = ex.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("Custom message is more important.");
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("errorCode", "COMMAND_NOT_IMPLEMENTED");
              });
    }

    @Test
    public void withCause() {
      Exception cause = new IllegalArgumentException("Cause message is important");
      JsonApiException ex = new JsonApiException(ErrorCode.COMMAND_NOT_IMPLEMENTED, cause);

      CommandResult result = ex.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .hasSize(2)
          .anySatisfy(
              error -> {
                assertThat(error.message()).isEqualTo("The provided command is not implemented.");
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("errorCode", "COMMAND_NOT_IMPLEMENTED");
              })
          .anySatisfy(
              error -> {
                assertThat(error.message()).isEqualTo("Cause message is important");
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "IllegalArgumentException");
              });
    }
  }
}
