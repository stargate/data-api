package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.vertx.core.impl.NoStackTraceTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EmbeddingProviderExceptionHandler}.
 *
 * <p>Companion to {@link RerankingProviderExceptionHandlerTest}: confirms the embedding handler
 * maps client-side timeouts to the {@code EMBEDDING_PROVIDER} scoped {@link
 * EmbeddingProviderException.Code#EMBEDDING_PROVIDER_TIMEOUT}, and a bad host name to {@link
 * EmbeddingProviderException.Code#EMBEDDING_PROVIDER_BAD_HOST_NAME}.
 */
public class EmbeddingProviderExceptionHandlerTest {

  private final EmbeddingProviderExceptionHandler handler =
      new EmbeddingProviderExceptionHandler(ModelProvider.NVIDIA, ModelType.EMBEDDING);

  @Test
  public void timeoutExceptionMapsToEmbeddingTimeout() {

    var actual = assertDoesNotThrow(() -> handler.maybeHandle(new TimeoutException("timed out")));

    assertThat(actual)
        .isInstanceOf(EmbeddingProviderException.class)
        .asInstanceOf(type(APIException.class))
        .satisfies(
            ex -> {
              assertThat(ex.code)
                  .isEqualTo(EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT.name());
              assertThat(ex.scope).isEqualTo(EmbeddingProviderException.SCOPE.scope());
              assertThat(ex.family).isEqualTo(ErrorFamily.SERVER);
            });
  }

  @Test
  public void noStackTraceTimeoutExceptionMapsToEmbeddingTimeout() {

    var vertxTimeout =
        new NoStackTraceTimeoutException("The timeout of 10 ms has been exceeded ...");

    var actual = assertDoesNotThrow(() -> handler.maybeHandle(vertxTimeout));

    assertThat(actual)
        .isInstanceOf(EmbeddingProviderException.class)
        .asInstanceOf(type(APIException.class))
        .satisfies(
            ex -> {
              assertThat(ex.code)
                  .isEqualTo(EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT.name());
              assertThat(ex.scope).isEqualTo(EmbeddingProviderException.SCOPE.scope());
            });
  }

  @Test
  public void unknownHostExceptionMapsToBadHostName() {

    var actual =
        assertDoesNotThrow(() -> handler.maybeHandle(new UnknownHostException("no-such-host")));

    assertThat(actual)
        .isInstanceOf(EmbeddingProviderException.class)
        .asInstanceOf(type(APIException.class))
        .satisfies(
            ex -> {
              assertThat(ex.code)
                  .isEqualTo(
                      EmbeddingProviderException.Code.EMBEDDING_PROVIDER_BAD_HOST_NAME.name());
              assertThat(ex.scope).isEqualTo(EmbeddingProviderException.SCOPE.scope());
            });
  }
}
