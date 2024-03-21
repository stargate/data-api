package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingProviderConfig;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(EmbeddingClientTestResource.class)
public class EmbeddingProviderErrorMessageTest {

  @Inject PropertyBasedEmbeddingProviderConfig config;

  @Nested
  class NVidiaEmbeddingClientTest {
    @Test
    public void test429() throws Exception {
      Throwable exception =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  "test")
              .vectorize(
                  List.of("429"), Optional.empty(), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_RATE_LIMITED)
          .hasFieldOrPropertyWithValue(
              "message",
              "The configured Embedding Provider for this collection is rate limiting your requests: Error Code : 429 response description : Too Many Requests");
    }

    @Test
    public void test4xx() throws Exception {
      Throwable exception =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  "test")
              .vectorize(
                  List.of("400"), Optional.empty(), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_INVALID_REQUEST)
          .hasFieldOrPropertyWithValue(
              "message",
              "The configured Embedding Provider for this collection refused to process the request, response was: Error Code : 400 response description : Bad Request");
    }

    @Test
    public void test5xx() throws Exception {
      Throwable exception =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  "test")
              .vectorize(
                  List.of("503"), Optional.empty(), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_SERVER_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The configured Embedding Provider for this collection encountered an error processing the request: Error Code : 503 response description : Service Unavailable");
    }

    @Test
    public void testRetryError() throws Exception {
      Throwable exception =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  "test")
              .vectorize(
                  List.of("408"), Optional.empty(), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_TIMEOUT)
          .hasFieldOrPropertyWithValue("message", "The configured Embedding Provider timed out.");
    }
  }
}
