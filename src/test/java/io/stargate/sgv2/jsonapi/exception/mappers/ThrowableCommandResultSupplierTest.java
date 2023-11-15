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
                assertThat(error.message()).isEqualTo("With dedicated message");
                assertThat(error.status()).isEqualTo(Response.Status.OK);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "RuntimeException");
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
                assertThat(error.message()).isEqualTo("With dedicated message");
                assertThat(error.status()).isEqualTo(Response.Status.OK);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "RuntimeException");
              })
          .anySatisfy(
              error -> {
                assertThat(error.message()).isEqualTo("Cause message is important");
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "IllegalArgumentException");
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
                assertThat(error.message()).isEqualTo("ALREADY_EXISTS");
                assertThat(error.status()).isEqualTo(Response.Status.OK);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "StatusRuntimeException");
              });
    }

    @Test
    public void authenticationError() {
      Exception ex = new StatusRuntimeException(io.grpc.Status.UNAUTHENTICATED);
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("UNAUTHENTICATED: Invalid token");
                assertThat(error.status()).isEqualTo(Response.Status.UNAUTHORIZED);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "StatusRuntimeException");
              });
    }

    @Test
    public void internalError() {
      Exception ex = new StatusRuntimeException(io.grpc.Status.INTERNAL);
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("INTERNAL");
                assertThat(error.status()).isEqualTo(Response.Status.INTERNAL_SERVER_ERROR);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "StatusRuntimeException");
              });
    }

    @Test
    public void unavailableError() {
      Exception ex = new StatusRuntimeException(io.grpc.Status.UNAVAILABLE);
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("UNAVAILABLE");
                assertThat(error.status()).isEqualTo(Response.Status.BAD_GATEWAY);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "StatusRuntimeException");
              });
    }

    @Test
    public void deadlineExceededError() {
      Exception ex = new StatusRuntimeException(io.grpc.Status.DEADLINE_EXCEEDED);
      ThrowableCommandResultSupplier supplier = new ThrowableCommandResultSupplier(ex);

      CommandResult result = supplier.get();

      assertThat(result.data()).isNull();
      assertThat(result.status()).isNull();
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("DEADLINE_EXCEEDED");
                assertThat(error.status()).isEqualTo(Response.Status.GATEWAY_TIMEOUT);
                assertThat(error.fields())
                    .hasSize(1)
                    .containsEntry("exceptionClass", "StatusRuntimeException");
              });
    }
  }
}
