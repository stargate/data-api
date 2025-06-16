package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * NOTE: this test relies on the {@link EmbeddingClientTestResource} to mock the server responses
 */
@QuarkusTest
@WithTestResource(EmbeddingClientTestResource.class)
public class OpenAiEmbeddingClientTest {

  @Inject EmbeddingProvidersConfig embeddingProvidersConfig;

  private final EmbeddingCredentials embeddingCredentials =
      new EmbeddingCredentials(
          "test-tenant", Optional.of("test"), Optional.empty(), Optional.empty());

  private final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig MODEL_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
          "test-model",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          Optional.of(123),
          List.of(),
          Map.of(),
          Optional.empty());

  private final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
              3, 10, 100, 100, 0.5, Optional.empty(), Optional.empty(), Optional.empty(), 10);

  private final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl PROVIDER_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
          ModelProvider.OPENAI.apiName(),
          true,
          Optional.of(EmbeddingClientTestResource.OPENAI_URL),
          false,
          Map.of(),
          List.of(),
          REQUEST_PROPERTIES,
          List.of());

  private final ServiceConfigStore.ServiceConfig SERVICE_CONFIG =
      new ServiceConfigStore.ServiceConfig(
          ModelProvider.OPENAI,
          EmbeddingClientTestResource.OPENAI_URL, // path important see EmbeddingProviderErrorMessageTest
          Optional.empty(),
          new ServiceConfigStore.ServiceRequestProperties(
              REQUEST_PROPERTIES.atMostRetries(),
              REQUEST_PROPERTIES.initialBackOffMillis(),
              REQUEST_PROPERTIES.readTimeoutMillis(),
              REQUEST_PROPERTIES.maxBackOffMillis(),
              REQUEST_PROPERTIES.jitter(),
              REQUEST_PROPERTIES.taskTypeRead(),
              REQUEST_PROPERTIES.taskTypeStore(),
              REQUEST_PROPERTIES.maxBatchSize()),
          Map.of());

  private OpenAIEmbeddingProvider createProvider(Map<String, Object> vectorizeServiceParameters) {
    return new OpenAIEmbeddingProvider(
        PROVIDER_CONFIG,
        MODEL_CONFIG,
        SERVICE_CONFIG,
        3,
        vectorizeServiceParameters);
  }

  private EmbeddingProvider.BatchedEmbeddingResponse runVectorize(
      EmbeddingProvider embeddingProvider, List<String> texts) {

    return embeddingProvider
        .vectorize(1, texts, embeddingCredentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem()
        .getItem();
  }

  private Throwable vectorizeWithError(EmbeddingProvider embeddingProvider, String text) {

    return embeddingProvider
        .vectorize(
            1, List.of(text), embeddingCredentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .getFailure();
  }

  @Nested
  class OpenAiEmbeddingTest {

    @Test
    public void happyPath() throws Exception {

      var response =
          runVectorize(
              createProvider(Map.of("organizationId", "org-id", "projectId", "project-id")),
              List.of("some data"));

      assertThat(response)
          .isInstanceOf(EmbeddingProvider.BatchedEmbeddingResponse.class)
          .satisfies(
              r -> {
                assertThat(r.embeddings()).isNotNull();
                assertThat(r.embeddings().size()).isEqualTo(1);
                assertThat(r.embeddings().get(0).length).isEqualTo(3);
              });
    }

    @Test
    public void onlyToken() throws Exception {

      var response = runVectorize(createProvider(Map.of()), List.of(MediaType.APPLICATION_JSON));

      assertThat(response)
          .isInstanceOf(EmbeddingProvider.BatchedEmbeddingResponse.class)
          .satisfies(
              r -> {
                assertThat(r.embeddings()).isNotNull();
                assertThat(r.embeddings().size()).isEqualTo(1);
                assertThat(r.embeddings().get(0).length).isEqualTo(3);
              });
    }

    @Test
    public void invalidOrg() throws Exception {

      var exception =
          vectorizeWithError(
              createProvider(Map.of("organizationId", "invalid org", "projectId", "project-id")),
              "some data");

      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned a HTTP client error: Provider: openai; HTTP Status: 401; Error Message: {\"object\":\"list\"}");
    }

    @Test
    public void invalidProject() throws Exception {

      var exception =
          vectorizeWithError(
              createProvider(Map.of("organizationId", "org-id", "projectId", "invalid proj")),
              "some data");

      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned a HTTP client error: Provider: openai; HTTP Status: 401; Error Message: {\"object\":\"list\"}");
    }
  }
}
