package io.stargate.sgv3.docsapi.exception.mappers;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
  }
}
