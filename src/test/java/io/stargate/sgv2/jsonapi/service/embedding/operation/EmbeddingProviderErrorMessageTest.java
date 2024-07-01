package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(EmbeddingClientTestResource.class)
public class EmbeddingProviderErrorMessageTest {
  private static final int DEFAULT_DIMENSIONS = 0;

  private final EmbeddingProvider.Credentials credentials =
      new EmbeddingProvider.Credentials(Optional.of("test"), Optional.empty(), Optional.empty());

  @Inject EmbeddingProvidersConfig config;

  @Nested
  class NvidiaEmbeddingProviderTest {
    @Test
    public void test429() throws Exception {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1, List.of("429"), credentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_RATE_LIMITED)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider rate limited the request: Provider: nvidia; HTTP Status: 429; Error Message: {\"object\":\"list\"}");
    }

    @Test
    public void test4xx() throws Exception {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1, List.of("400"), credentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_CLIENT_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned a HTTP client error: Provider: nvidia; HTTP Status: 400; Error Message: {\"object\":\"list\"}");
    }

    @Test
    public void test5xx() throws Exception {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1, List.of("503"), credentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_SERVER_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned a HTTP server error: Provider: nvidia; HTTP Status: 503; Error Message: {\"object\":\"list\"}");
    }

    @Test
    public void testRetryError() throws Exception {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1, List.of("408"), credentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.EMBEDDING_PROVIDER_TIMEOUT)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider timed out: Provider: nvidia; HTTP Status: 408; Error Message: {\"object\":\"list\"}");
    }

    @Test
    public void testCorrectHeaderAndBody() {
      final EmbeddingProvider.Response result =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1,
                  List.of("application/json"),
                  credentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      assertThat(result).isNotNull();
      assertThat(result.batchId()).isEqualTo(1);
      assertThat(result.embeddings()).isNotNull();
    }

    @Test
    public void testIncorrectContentTypeXML() {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1,
                  List.of("application/xml"),
                  credentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned an unexpected response: Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found 'application/xml'; HTTP Status: 200; The response body is: '<object>list</object>'.");
    }

    @Test
    public void testIncorrectContentTypePlainText() {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1,
                  List.of("text/plain;charset=UTF-8"),
                  credentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned an unexpected response: Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found 'text/plain;charset=UTF-8'; HTTP Status: 500; The response body is: 'Not Found'.");
    }

    @Test
    public void testNoJsonResponse() {
      Throwable exception =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1,
                  List.of("no json body"),
                  credentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception.getCause())
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue(
              "errorCode", ErrorCode.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned an unexpected response: No response body from the embedding provider");
    }

    @Test
    public void testEmptyJsonResponse() {
      final EmbeddingProvider.Response result =
          new NvidiaEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("nvidia").url(),
                  "test",
                  DEFAULT_DIMENSIONS,
                  null)
              .vectorize(
                  1,
                  List.of("empty json body"),
                  credentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      assertThat(result).isNotNull();
      assertThat(result.batchId()).isEqualTo(1);
      assertThat(result.embeddings()).isNotNull();
    }
  }
}
