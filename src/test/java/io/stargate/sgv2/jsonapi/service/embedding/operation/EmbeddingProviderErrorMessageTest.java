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
                  null)
              .vectorize(
                  List.of("429"), Optional.of("test"), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
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
                  null)
              .vectorize(
                  List.of("400"), Optional.of("test"), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
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
                  null)
              .vectorize(
                  List.of("503"), Optional.of("test"), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
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
                  null)
              .vectorize(
                  List.of("408"), Optional.of("test"), EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_TIMEOUT)
          .hasFieldOrPropertyWithValue("message", "The configured Embedding Provider timed out.");
    }

    @Test
    public void testCorrectHeaderAndBody() {
      List<float[]> result =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  null)
              .vectorize(
                  List.of("application/json"),
                  Optional.of("test"),
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      assertThat(result).isNotNull();
    }

    @Test
    public void testIncorrectContentType() {
      Throwable exception =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  null)
              .vectorize(
                  List.of("application/xml"),
                  Optional.of("test"),
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_INVALID_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The configured Embedding Provider for this collection return an invalid response: Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found 'application/xml'");
    }

    @Test
    public void testNoJsonResponse() {
      Throwable exception =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  null)
              .vectorize(
                  List.of("no json body"),
                  Optional.of("test"),
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_INVALID_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The configured Embedding Provider for this collection return an invalid response: No JSON body from the embedding provider");
    }

    @Test
    public void testEmptyJsonResponse() {
      List<float[]> result =
          new NVidiaEmbeddingClient(
                  EmbeddingProviderConfigStore.RequestProperties.of(2, 100, 3000),
                  config.providers().get("nvidia").url(),
                  "test",
                  null)
              .vectorize(
                  List.of("empty json body"),
                  Optional.of("test"),
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      assertThat(result).isEmpty();
    }
  }
}
