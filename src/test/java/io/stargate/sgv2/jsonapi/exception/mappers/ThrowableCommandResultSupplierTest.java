package io.stargate.sgv2.jsonapi.exception.mappers;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class ThrowableCommandResultSupplierTest {

  @Nested
  class Get {

    @Test
    public void happyPath() {
      Exception ex = new RuntimeException("With dedicated message");
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (java.lang.RuntimeException) With dedicated message");
                assertThat(error.status()).isEqualTo(Response.Status.INTERNAL_SERVER_ERROR);
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }

    @Test
    public void withCause() {
      Exception cause = new IllegalArgumentException("Cause message is important");
      Exception ex = new RuntimeException("With dedicated message", cause);
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .hasSize(2)
          .anySatisfy(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (java.lang.RuntimeException) With dedicated message");
                assertThat(error.status()).isEqualTo(Response.Status.INTERNAL_SERVER_ERROR);
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
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

    @Test
    public void statusRuntimeException() {
      Exception ex = new StatusRuntimeException(Status.ALREADY_EXISTS);
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (io.grpc.StatusRuntimeException) ALREADY_EXISTS");
                assertThat(error.status()).isEqualTo(Response.Status.INTERNAL_SERVER_ERROR);
                assertThat(error.fields())
                    .hasSize(2)
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }
  }
}
