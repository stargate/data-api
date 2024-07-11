package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.gateway.EmbeddingGatewayClient;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class EmbeddingGatewayClientTest {

  public static final String TESTING_COMMAND_NAME = "test_command";

  // for [data-api#1088] (NPE for VoyageAI provider)
  @Test
  void verifyDirectConstructionWithNullServiceParameters() {
    List<EmbeddingProviderFactory.ProviderConstructor> providerCtors =
        Arrays.asList(
            AzureOpenAIEmbeddingProvider::new,
            CohereEmbeddingProvider::new,
            HuggingFaceEmbeddingProvider::new,
            JinaAIEmbeddingProvider::new,
            MistralEmbeddingProvider::new,
            NvidiaEmbeddingProvider::new,
            OpenAIEmbeddingProvider::new,
            UpstageAIEmbeddingProvider::new,
            VertexAIEmbeddingProvider::new,
            VoyageAIEmbeddingProvider::new);
    for (EmbeddingProviderFactory.ProviderConstructor ctor : providerCtors) {
      EmbeddingProviderConfigStore.RequestProperties requestProperties =
          EmbeddingProviderConfigStore.RequestProperties.of(
              3, 5, 5000, 5, 0.5, Optional.empty(), Optional.empty(), 2048);
      assertThat(ctor.create(requestProperties, "baseUrl", "modelName", 5, null)).isNotNull();
    }
  }

  @Test
  void handleValidResponse() {
    EmbeddingService embeddingService = mock(EmbeddingService.class);
    final EmbeddingGateway.EmbeddingResponse.Builder builder =
        EmbeddingGateway.EmbeddingResponse.newBuilder();
    EmbeddingGateway.EmbeddingResponse.FloatEmbedding.Builder floatEmbeddingBuilder =
        EmbeddingGateway.EmbeddingResponse.FloatEmbedding.newBuilder();
    floatEmbeddingBuilder
        .addEmbedding(0.5f)
        .addEmbedding(0.5f)
        .addEmbedding(0.5f)
        .addEmbedding(0.5f)
        .addEmbedding(0.5f);
    builder
        .addEmbeddings(floatEmbeddingBuilder.build())
        .addEmbeddings(floatEmbeddingBuilder.build());

    when(embeddingService.embed(any())).thenReturn(Uni.createFrom().item(builder.build()));
    EmbeddingGatewayClient embeddingGatewayClient =
        new EmbeddingGatewayClient(
            EmbeddingProviderConfigStore.RequestProperties.of(
                5, 5, 5, 5, 0.5, Optional.empty(), Optional.empty(), 2048),
            "openai",
            1536,
            Optional.of("default"),
            Optional.of("default"),
            "https://api.openai.com/v1/",
            "text-embedding-3-small",
            embeddingService,
            Map.of(),
            Map.of(),
            TESTING_COMMAND_NAME);

    final EmbeddingProvider.Response response =
        embeddingGatewayClient
            .vectorize(
                1,
                List.of("data 1", "data 2"),
                null,
                EmbeddingGatewayClient.EmbeddingRequestType.INDEX)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    assertThat(response).isNotNull();
    assertThat(response.batchId()).isEqualTo(1);
    assertThat(response.embeddings()).isNotEmpty();
    assertThat(response.embeddings().size()).isEqualTo(2);
    assertThat(response.embeddings().get(0).length).isEqualTo(5);
    assertThat(response.embeddings().get(1).length).isEqualTo(5);
  }

  @Test
  void handleError() {
    EmbeddingService embeddingService = mock(EmbeddingService.class);
    final EmbeddingGateway.EmbeddingResponse.Builder builder =
        EmbeddingGateway.EmbeddingResponse.newBuilder();
    EmbeddingGateway.EmbeddingResponse.ErrorResponse.Builder errorResponseBuilder =
        EmbeddingGateway.EmbeddingResponse.ErrorResponse.newBuilder();
    final JsonApiException apiException =
        ErrorCode.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
            "Error Code : %s response description : %s", 429, "Too Many Requests");
    errorResponseBuilder
        .setErrorCode(apiException.getErrorCode().name())
        .setErrorMessage(apiException.getMessage());
    builder.setError(errorResponseBuilder.build());
    when(embeddingService.embed(any())).thenReturn(Uni.createFrom().item(builder.build()));
    EmbeddingGatewayClient embeddingGatewayClient =
        new EmbeddingGatewayClient(
            EmbeddingProviderConfigStore.RequestProperties.of(
                5, 5, 5, 5, 0.5, Optional.empty(), Optional.empty(), 2048),
            "openai",
            1536,
            Optional.of("default"),
            Optional.of("default"),
            "https://api.openai.com/v1/",
            "text-embedding-3-small",
            embeddingService,
            Map.of(),
            Map.of(),
            TESTING_COMMAND_NAME);

    Throwable result =
        embeddingGatewayClient
            .vectorize(
                1,
                List.of("data 1", "data 2"),
                null,
                EmbeddingGatewayClient.EmbeddingRequestType.INDEX)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(result)
        .isInstanceOf(JsonApiException.class)
        .satisfies(
            e -> {
              JsonApiException exception = (JsonApiException) e;
              assertThat(exception.getMessage()).isEqualTo(apiException.getMessage());
              assertThat(exception.getErrorCode()).isEqualTo(apiException.getErrorCode());
            });
  }
}
