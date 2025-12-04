package io.stargate.sgv2.jsonapi.service.reranking.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelType;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Tests for the RerankEGWClient class. Mocking the embedding gateway service to test the grpc
 * rerank API.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class RerankingGatewayClientTest {

  private static final TestConstants testConstants = new TestConstants();

  public static final String TESTING_COMMAND_NAME = "test_command";

  private static final RerankingCredentials RERANK_CREDENTIALS =
      new RerankingCredentials(testConstants.TENANT, "mocked reranking api key");

  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10);

  private static final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig MODEL_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
          "testModel",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          false,
          "http://testing.com",
          REQUEST_PROPERTIES);

  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl PROVIDER_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
          false, "test", true, Map.of(), List.of());

  @Test
  void handleValidResponse() {
    RerankingService rerankService = mock(RerankingService.class);
    final EmbeddingGateway.RerankingResponse.Builder builder =
        EmbeddingGateway.RerankingResponse.newBuilder();
    // Mocking ranks
    List<Integer> indices = List.of(1, 0);
    List<Float> scores = List.of(1f, 0.1f);

    List<EmbeddingGateway.RerankingResponse.Rank> ranks =
        IntStream.range(0, indices.size())
            .mapToObj(
                i ->
                    EmbeddingGateway.RerankingResponse.Rank.newBuilder()
                        .setIndex(indices.get(i))
                        .setScore(scores.get(i))
                        .build())
            .toList();
    builder.addAllRanks(ranks);
    // mock model usage
    builder.setModelUsage(
        EmbeddingGateway.ModelUsage.newBuilder()
            .setModelType(EmbeddingGateway.ModelUsage.ModelType.RERANKING)
            .setModelProvider(ModelProvider.NVIDIA.apiName())
            .setModelName("llama-3.2-nv-rerankqa-1b-v2")
            .setPromptTokens(10)
            .setTotalTokens(20)
            .setRequestBytes(100)
            .setResponseBytes(200)
            .build());
    when(rerankService.rerank(any())).thenReturn(Uni.createFrom().item(builder.build()));

    // Create a RerankEGWClient instance
    RerankingEGWClient rerankEGWClient =
        new RerankingEGWClient(
            ModelProvider.NVIDIA,
            MODEL_CONFIG,
            testConstants.TENANT,
            "default",
            rerankService,
            Map.of(),
            TESTING_COMMAND_NAME);

    final RerankingProvider.BatchedRerankingResponse response =
        rerankEGWClient
            .rerank(1, "apple", List.of("orange", "apple"), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    assertThat(response).isNotNull();
    assertThat(response.batchId()).isEqualTo(1);
    assertThat(response.ranks()).isNotEmpty();
    assertThat(response.ranks().size()).isEqualTo(2);
    assertThat(response.ranks().get(0).index()).isEqualTo(1);
    assertThat(response.ranks().get(0).score()).isEqualTo(1f);
    assertThat(response.ranks().get(1).index()).isEqualTo(0);
    assertThat(response.ranks().get(1).score()).isEqualTo(0.1f);

    assertThat(response.modelUsage()).isNotNull();
    assertThat(response.modelUsage().modelType()).isEqualTo(ModelType.RERANKING);
    assertThat(response.modelUsage().modelProvider()).isEqualTo(ModelProvider.NVIDIA);
    assertThat(response.modelUsage().modelName()).isEqualTo("llama-3.2-nv-rerankqa-1b-v2");
    assertThat(response.modelUsage().promptTokens()).isEqualTo(10);
    assertThat(response.modelUsage().totalTokens()).isEqualTo(20);
    assertThat(response.modelUsage().requestBytes()).isEqualTo(100);
    assertThat(response.modelUsage().responseBytes()).isEqualTo(200);
  }

  @Test
  void handleError() {

    RerankingService rerankService = mock(RerankingService.class);
    final EmbeddingGateway.RerankingResponse.Builder builder =
        EmbeddingGateway.RerankingResponse.newBuilder();
    EmbeddingGateway.RerankingResponse.ErrorResponse.Builder errorResponseBuilder =
        EmbeddingGateway.RerankingResponse.ErrorResponse.newBuilder();
    final JsonApiException apiException =
        ErrorCodeV1.RERANKING_PROVIDER_SERVER_ERROR.toApiException();
    errorResponseBuilder
        .setErrorCode(apiException.getErrorCode().name())
        .setErrorMessage(apiException.getMessage());
    builder.setError(errorResponseBuilder.build());
    when(rerankService.rerank(any())).thenReturn(Uni.createFrom().item(builder.build()));

    // Create a RerankEGWClient instance
    RerankingEGWClient rerankEGWClient =
        new RerankingEGWClient(
            ModelProvider.NVIDIA,
            MODEL_CONFIG,
            testConstants.TENANT,
            "default",
            rerankService,
            Map.of(),
            TESTING_COMMAND_NAME);

    Throwable result =
        rerankEGWClient
            .rerank(1, "apple", List.of("orange", "apple"), RERANK_CREDENTIALS)
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
