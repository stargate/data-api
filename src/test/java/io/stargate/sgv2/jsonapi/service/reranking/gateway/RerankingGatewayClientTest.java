package io.stargate.sgv2.jsonapi.service.reranking.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
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
            .setTenantId(testConstants.TENANT.toString())
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
  void mapsSchemaErrorFromGateway() {

    RerankingService rerankService = mock(RerankingService.class);
    final EmbeddingGateway.RerankingResponse.Builder builder =
        EmbeddingGateway.RerankingResponse.newBuilder();
    EmbeddingGateway.RerankingResponse.ErrorResponse.Builder errorResponseBuilder =
        EmbeddingGateway.RerankingResponse.ErrorResponse.newBuilder();
    final SchemaException apiException =
        SchemaException.Code.RERANKING_PROVIDER_SERVER_ERROR.get(
            Map.of("errorMessage", "Test fail"));
    errorResponseBuilder
        .setErrorCode(apiException.code)
        .setErrorTitle("Gateway schema title")
        .setErrorBody(apiException.getMessage());
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
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e -> {
              SchemaException exception = (SchemaException) e;
              assertThat(exception.family).isEqualTo(ErrorFamily.REQUEST);
              assertThat(exception.scope).isEqualTo(SchemaException.SCOPE.scope());
              assertThat(exception.code).isEqualTo(apiException.code);
              assertThat(exception.title).isEqualTo("Reranking provider server error");
              assertThat(exception.body).isEqualTo(apiException.body);
            });
  }

  @Test
  void mapsServerErrorFromGateway() {
    RerankingService rerankService = mock(RerankingService.class);
    when(rerankService.rerank(any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    gatewayErrorResponse(
                        ServerException.Code.UNEXPECTED_SERVER_ERROR.name(),
                        "Gateway server error",
                        "Gateway server error body")));

    Throwable result = rerankAndAwaitFailure(rerankService);

    assertThat(result)
        .isInstanceOf(ServerException.class)
        .satisfies(
            failure -> {
              ServerException exception = (ServerException) failure;
              assertThat(exception.family).isEqualTo(ErrorFamily.SERVER);
              assertThat(exception.scope).isEmpty();
              assertThat(exception.code)
                  .isEqualTo(ServerException.Code.UNEXPECTED_SERVER_ERROR.name());
              assertThat(exception.title).isEqualTo("Unexpected server error");
              assertThat(exception.body).isEqualTo("Gateway server error body");
            });
  }

  @Test
  void mapsRerankingProviderErrorFromGateway() {
    RerankingService rerankService = mock(RerankingService.class);
    when(rerankService.rerank(any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    gatewayErrorResponse(
                        RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name(),
                        "Gateway timeout",
                        "Gateway timeout body")));

    Throwable result = rerankAndAwaitFailure(rerankService);

    assertThat(result)
        .isInstanceOf(RerankingProviderException.class)
        .satisfies(
            failure -> {
              RerankingProviderException exception = (RerankingProviderException) failure;
              assertThat(exception.family).isEqualTo(ErrorFamily.SERVER);
              assertThat(exception.scope).isEqualTo(RerankingProviderException.SCOPE.scope());
              assertThat(exception.code)
                  .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
              assertThat(exception.title).isEqualTo("Reranking Provider timed out");
              assertThat(exception.body).isEqualTo("Gateway timeout body");
            });
  }

  @Test
  void preservesUnknownGatewayError() {
    RerankingService rerankService = mock(RerankingService.class);
    when(rerankService.rerank(any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    gatewayErrorResponse(
                        "FUTURE_GATEWAY_ERROR",
                        "Future gateway error",
                        "Future gateway error body")));

    Throwable result = rerankAndAwaitFailure(rerankService);

    assertThat(result)
        .isInstanceOf(RerankingProviderException.class)
        .satisfies(
            failure -> {
              RerankingProviderException exception = (RerankingProviderException) failure;
              assertThat(exception.family).isEqualTo(ErrorFamily.SERVER);
              assertThat(exception.scope).isEqualTo(RerankingProviderException.SCOPE.scope());
              assertThat(exception.code).isEqualTo("FUTURE_GATEWAY_ERROR");
              assertThat(exception.title).isEqualTo("Future gateway error");
              assertThat(exception.body).isEqualTo("Future gateway error body");
            });
  }

  @Test
  void mapsAsyncDeadlineExceeded() {
    RerankingService rerankService = mock(RerankingService.class);
    when(rerankService.rerank(any()))
        .thenReturn(Uni.createFrom().failure(Status.DEADLINE_EXCEEDED.asRuntimeException()));

    Throwable result = rerankAndAwaitFailure(rerankService);

    assertThat(result)
        .isInstanceOf(RerankingProviderException.class)
        .satisfies(
            failure -> {
              RerankingProviderException exception = (RerankingProviderException) failure;
              assertThat(exception.code)
                  .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
              assertThat(exception.body).contains(ModelProvider.NVIDIA.apiName());
              assertThat(exception.body).contains(Status.Code.DEADLINE_EXCEEDED.name());
            });
  }

  @Test
  void mapsSynchronousDeadlineExceeded() {
    RerankingService rerankService = mock(RerankingService.class);
    when(rerankService.rerank(any())).thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException());

    Throwable result = rerankAndAwaitFailure(rerankService);

    assertThat(result)
        .isInstanceOf(RerankingProviderException.class)
        .satisfies(
            failure -> {
              RerankingProviderException exception = (RerankingProviderException) failure;
              assertThat(exception.code)
                  .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
              assertThat(exception.body).contains(Status.Code.DEADLINE_EXCEEDED.name());
            });
  }

  @Test
  void preservesAsyncUnavailableStatusFailure() {
    RerankingService rerankService = mock(RerankingService.class);
    StatusRuntimeException unavailable = Status.UNAVAILABLE.asRuntimeException();
    when(rerankService.rerank(any())).thenReturn(Uni.createFrom().failure(unavailable));

    Throwable result = rerankAndAwaitFailure(rerankService);

    assertThat(result).isSameAs(unavailable);
  }

  private static Throwable rerankAndAwaitFailure(RerankingService rerankService) {
    return createClient(rerankService)
        .rerank(1, "apple", List.of("orange", "apple"), RERANK_CREDENTIALS)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .getFailure();
  }

  private static RerankingEGWClient createClient(RerankingService rerankService) {
    return new RerankingEGWClient(
        ModelProvider.NVIDIA,
        MODEL_CONFIG,
        testConstants.TENANT,
        "default",
        rerankService,
        Map.of(),
        TESTING_COMMAND_NAME);
  }

  private static EmbeddingGateway.RerankingResponse gatewayErrorResponse(
      String code, String title, String body) {
    return EmbeddingGateway.RerankingResponse.newBuilder()
        .setError(
            EmbeddingGateway.RerankingResponse.ErrorResponse.newBuilder()
                .setErrorCode(code)
                .setErrorTitle(title)
                .setErrorBody(body))
        .build();
  }
}
