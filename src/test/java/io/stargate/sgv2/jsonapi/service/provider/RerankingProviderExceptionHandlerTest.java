package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.vertx.core.impl.NoStackTraceTimeoutException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RerankingProviderExceptionHandler}.
 *
 * <p>Regression coverage for <a href="https://github.com/stargate/data-api/issues/2134">#2134</a>:
 * a client-side timeout while calling a reranking provider must map to the {@code
 * RERANKING_PROVIDER} scoped {@link RerankingProviderException.Code#RERANKING_PROVIDER_TIMEOUT},
 * and must NOT leak through as the {@code EMBEDDING_PROVIDER} scoped {@code
 * EMBEDDING_PROVIDER_TIMEOUT} or a generic {@code UNEXPECTED_SERVER_ERROR}.
 */
public class RerankingProviderExceptionHandlerTest {

  private final RerankingProviderExceptionHandler handler =
      new RerankingProviderExceptionHandler(ModelProvider.NVIDIA, ModelType.RERANKING);

  @Test
  public void timeoutExceptionMapsToRerankingTimeout() {

    var actual = assertDoesNotThrow(() -> handler.maybeHandle(new TimeoutException("timed out")));

    assertThat(actual)
        .as("TimeoutException maps to a RERANKING_PROVIDER scoped reranking timeout")
        .isInstanceOf(RerankingProviderException.class)
        .asInstanceOf(type(APIException.class))
        .satisfies(
            ex -> {
              assertThat(ex.code)
                  .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
              assertThat(ex.scope).isEqualTo(RerankingProviderException.SCOPE.scope());
              assertThat(ex.family).isEqualTo(ErrorFamily.SERVER);
            });
  }

  /**
   * The Quarkus reactive REST client raises a {@link NoStackTraceTimeoutException} (a subclass of
   * {@link TimeoutException}) on a client-side read/connection timeout, which is the exact
   * exception reported in #2134. It must be routed the same way as a plain {@link
   * TimeoutException}.
   */
  @Test
  public void noStackTraceTimeoutExceptionMapsToRerankingTimeout() {

    var vertxTimeout =
        new NoStackTraceTimeoutException("The timeout of 10 ms has been exceeded ...");

    var actual = assertDoesNotThrow(() -> handler.maybeHandle(vertxTimeout));

    assertThat(actual)
        .as("Vert.x NoStackTraceTimeoutException maps to the reranking timeout, not embedding")
        .isInstanceOf(RerankingProviderException.class)
        .asInstanceOf(type(APIException.class))
        .satisfies(
            ex -> {
              assertThat(ex.code)
                  .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
              assertThat(ex.scope)
                  .as("#2134: must be reported with the RERANKING_PROVIDER scope, not EMBEDDING")
                  .isEqualTo(RerankingProviderException.SCOPE.scope());
              assertThat(ex.family).isEqualTo(ErrorFamily.SERVER);
            });
  }
}
