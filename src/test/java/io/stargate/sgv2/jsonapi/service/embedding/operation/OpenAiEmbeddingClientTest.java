package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithTestResource(EmbeddingClientTestResource.class)
public class OpenAiEmbeddingClientTest {

  @Inject EmbeddingProvidersConfig config;

  private final EmbeddingCredentials embeddingCredentials =
      new EmbeddingCredentials(
          "test-tenant", Optional.of("test"), Optional.empty(), Optional.empty());

  private final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig testModel =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
          "test-model",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          Optional.of(123),
          List.of(),
          Map.of(),
          Optional.empty());

  @Nested
  class OpenAiEmbeddingTest {
    @Test
    public void happyPath() throws Exception {
      final EmbeddingProvider.BatchedEmbeddingResponse response =
          new OpenAIEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("openai").url().get(),
                  testModel,
                  3,
                  Map.of("organizationId", "org-id", "projectId", "project-id"),
                  null)
              .vectorize(
                  1,
                  List.of("some data"),
                  embeddingCredentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
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
      final EmbeddingProvider.BatchedEmbeddingResponse response =
          new OpenAIEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("openai").url().get(),
                  testModel,
                  3,
                  Map.of(),
                  null)
              .vectorize(
                  1,
                  List.of(MediaType.APPLICATION_JSON),
                  embeddingCredentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
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
      Throwable exception =
          new OpenAIEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("openai").url().get(),
                  testModel,
                  3,
                  Map.of("organizationId", "invalid org", "projectId", "project-id"),
                  null)
              .vectorize(
                  1,
                  List.of("some data"),
                  embeddingCredentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned a HTTP client error: Provider: openai; HTTP Status: 401; Error Message: {\"object\":\"list\"}");
    }

    @Test
    public void invalidProject() throws Exception {
      Throwable exception =
          new OpenAIEmbeddingProvider(
                  EmbeddingProviderConfigStore.RequestProperties.of(
                      2, 100, 3000, 100, 0.5, Optional.empty(), Optional.empty(), 10),
                  config.providers().get("openai").url().get(),
                  testModel,
                  3,
                  Map.of("organizationId", "org-id", "projectId", "invalid proj"),
                  null)
              .vectorize(
                  1,
                  List.of("some data"),
                  embeddingCredentials,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      assertThat(exception)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR)
          .hasFieldOrPropertyWithValue(
              "message",
              "The Embedding Provider returned a HTTP client error: Provider: openai; HTTP Status: 401; Error Message: {\"object\":\"list\"}");
    }
  }
}
