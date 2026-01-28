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
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.gateway.EmbeddingGatewayClient;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelType;
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

  private static final TestConstants testConstants = new TestConstants();

  private final EmbeddingCredentials embeddingCredentials =
      new EmbeddingCredentials(
          testConstants.TENANT, Optional.empty(), Optional.empty(), Optional.empty());

  private static final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig MODEL_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
          "testModel",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          Optional.empty(),
          List.of(),
          Map.of(),
          Optional.empty());

  private static final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
              3, 10, 100, 100, 0.5, Optional.empty(), Optional.empty(), Optional.empty(), 10);

  private static final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl PROVIDER_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
          ModelProvider.CUSTOM.apiName(),
          true,
          Optional.of("http://testing.com"),
          false,
          Map.of(),
          List.of(),
          REQUEST_PROPERTIES,
          List.of());

  private final ServiceConfigStore.ServiceConfig SERVICE_CONFIG =
      new ServiceConfigStore.ServiceConfig(
          ModelProvider.CUSTOM,
          "http://testing.com",
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
          null); // aaron -passing null here to bypass url overrides

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

      assertThat(ctor.create(PROVIDER_CONFIG, MODEL_CONFIG, SERVICE_CONFIG, 5, null)).isNotNull();
    }
  }

  @Test
  void handleValidResponse() {

    var floatEmbeddingBuilder =
        EmbeddingGateway.EmbeddingResponse.FloatEmbedding.newBuilder()
            .addEmbedding(0.5f)
            .addEmbedding(0.5f)
            .addEmbedding(0.5f)
            .addEmbedding(0.5f)
            .addEmbedding(0.5f);

    var modelUsageBuilder =
        EmbeddingGateway.ModelUsage.newBuilder()
            .setModelProvider(ModelProvider.OPENAI.apiName())
            .setModelType(EmbeddingGateway.ModelUsage.ModelType.EMBEDDING)
            .setModelName("test-model")
            .setTenantId(testConstants.TENANT.toString())
            .setInputType(EmbeddingGateway.ModelUsage.InputType.INDEX)
            .setPromptTokens(5)
            .setTotalTokens(5)
            .setRequestBytes(100)
            .setResponseBytes(100)
            .setCallDurationNanos(20000);

    var embeddingResonseBuilder =
        EmbeddingGateway.EmbeddingResponse.newBuilder()
            .addEmbeddings(floatEmbeddingBuilder.build())
            .addEmbeddings(floatEmbeddingBuilder.build())
            .setModelUsage(modelUsageBuilder.build());

    EmbeddingService embeddingService = mock(EmbeddingService.class);
    when(embeddingService.embed(any()))
        .thenReturn(Uni.createFrom().item(embeddingResonseBuilder.build()));

    EmbeddingGatewayClient embeddingGatewayClient =
        new EmbeddingGatewayClient(
            ModelProvider.OPENAI,
            PROVIDER_CONFIG,
            MODEL_CONFIG,
            SERVICE_CONFIG,
            1536,
            Map.of(),
            testConstants.TENANT,
            "default",
            embeddingService,
            Map.of(),
            TESTING_COMMAND_NAME);

    final EmbeddingProvider.BatchedEmbeddingResponse response =
        embeddingGatewayClient
            .vectorize(
                1,
                List.of("data 1", "data 2"),
                embeddingCredentials,
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

    assertThat(response.modelUsage()).isNotNull();
    assertThat(response.modelUsage().modelProvider()).isEqualTo(ModelProvider.OPENAI);
    assertThat(response.modelUsage().modelType()).isEqualTo(ModelType.EMBEDDING);
    assertThat(response.modelUsage().modelName()).isEqualTo("test-model");
    assertThat(response.modelUsage().tenant()).isEqualTo(testConstants.TENANT);
    assertThat(response.modelUsage().inputType()).isEqualTo(ModelInputType.INDEX);

    assertThat(response.modelUsage().promptTokens()).isEqualTo(5);
    assertThat(response.modelUsage().totalTokens()).isEqualTo(5);
    assertThat(response.modelUsage().requestBytes()).isEqualTo(100);
    assertThat(response.modelUsage().responseBytes()).isEqualTo(100);
    assertThat(response.modelUsage().durationNanos()).isEqualTo(20000);
    assertThat(response.modelUsage().batchCount()).isEqualTo(1);
  }

  @Test
  void handleError() {
    EmbeddingService embeddingService = mock(EmbeddingService.class);
    EmbeddingGateway.EmbeddingResponse.Builder builder =
        EmbeddingGateway.EmbeddingResponse.newBuilder();
    EmbeddingGateway.EmbeddingResponse.ErrorResponse.Builder errorResponseBuilder =
        EmbeddingGateway.EmbeddingResponse.ErrorResponse.newBuilder();
    APIException apiException =
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_RATE_LIMITED.get(
            Map.of(
                "provider", "TEST-MODEL", "httpStatus", "429", "errorMessage", "Slow Down Dude!"));

    errorResponseBuilder
        .setErrorCode(apiException.code)
        .setErrorTitle(apiException.title)
        .setErrorBody(apiException.body);
    builder.setError(errorResponseBuilder.build());
    when(embeddingService.embed(any())).thenReturn(Uni.createFrom().item(builder.build()));

    EmbeddingGatewayClient embeddingGatewayClient =
        new EmbeddingGatewayClient(
            ModelProvider.OPENAI,
            PROVIDER_CONFIG,
            MODEL_CONFIG,
            SERVICE_CONFIG,
            1536,
            Map.of(),
            testConstants.TENANT,
            "default",
            embeddingService,
            Map.of(),
            TESTING_COMMAND_NAME);

    Throwable result =
        embeddingGatewayClient
            .vectorize(
                1,
                List.of("data 1", "data 2"),
                embeddingCredentials,
                EmbeddingGatewayClient.EmbeddingRequestType.INDEX)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(result)
        .isInstanceOf(APIException.class)
        .satisfies(
            e -> {
              APIException exception = (APIException) e;
              assertThat(exception.getMessage()).isEqualTo(apiException.getMessage());
              assertThat(exception.code).isEqualTo(apiException.code);
            });
  }
}
