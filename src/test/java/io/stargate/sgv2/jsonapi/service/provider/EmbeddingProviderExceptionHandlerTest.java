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
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  private static Stream<Arguments> handledExceptions() {
    return Stream.of(
        Arguments.of(
            new TimeoutException("timed out"),
            EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT),
        // Quarkus reactive REST client raises this TimeoutException subclass on a client-side
        // read/connection timeout, the exact exception type behind the embedding side of #2134.
        Arguments.of(
            new NoStackTraceTimeoutException("The timeout of 10 ms has been exceeded ..."),
            EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT),
        Arguments.of(
            new UnknownHostException("no-such-host"),
            EmbeddingProviderException.Code.EMBEDDING_PROVIDER_BAD_HOST_NAME));
  }

  @ParameterizedTest
  @MethodSource("handledExceptions")
  public void mapsToEmbeddingProviderError(
      Throwable input, EmbeddingProviderException.Code expectedCode) {

    var actual = assertDoesNotThrow(() -> handler.maybeHandle(input));

    assertThat(actual)
        .as("%s maps to %s", input.getClass().getSimpleName(), expectedCode.name())
        .isInstanceOf(EmbeddingProviderException.class)
        .asInstanceOf(type(APIException.class))
        .satisfies(
            ex -> {
              assertThat(ex.code).isEqualTo(expectedCode.name());
              assertThat(ex.scope).isEqualTo(EmbeddingProviderException.SCOPE.scope());
              assertThat(ex.family).isEqualTo(ErrorFamily.SERVER);
            });
  }
}
