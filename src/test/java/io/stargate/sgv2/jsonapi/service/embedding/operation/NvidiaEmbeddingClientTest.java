package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@code dimensions} parameter is sent to the Nvidia NIM only for variable-dimension
 * models. Relies on {@link EmbeddingClientTestResource}: input text must contain {@code
 * application/json} to get the mocked success response.
 */
@QuarkusTest
@WithTestResource(EmbeddingClientTestResource.class)
public class NvidiaEmbeddingClientTest {

  private static final String VARIABLE_DIMENSION_MODEL = "nvidia/llama-3.2-nv-embedqa-1b-v2";
  private static final String FIXED_DIMENSION_MODEL = "NV-Embed-QA";

  private final TestConstants testConstants = new TestConstants();

  private final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig
      VARIABLE_DIMENSION_MODEL_CONFIG =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
              VARIABLE_DIMENSION_MODEL,
              new ApiModelSupport.ApiModelSupportImpl(
                  ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
              Optional.empty(),
              List.of(),
              Map.of(),
              Optional.empty());

  private final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig
      FIXED_DIMENSION_MODEL_CONFIG =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
              FIXED_DIMENSION_MODEL,
              new ApiModelSupport.ApiModelSupportImpl(
                  ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
              Optional.of(1024),
              List.of(),
              Map.of(),
              Optional.empty());

  private final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
              3, 10, 100, 100, 0.5, Optional.empty(), Optional.empty(), Optional.empty(), 10);

  private final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl PROVIDER_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
          ModelProvider.NVIDIA.apiName(),
          true,
          Optional.of(EmbeddingClientTestResource.NVIDIA_URL),
          false,
          Map.of(),
          List.of(),
          REQUEST_PROPERTIES,
          List.of());

  private final ServiceConfigStore.ServiceConfig SERVICE_CONFIG =
      new ServiceConfigStore.ServiceConfig(
          ModelProvider.NVIDIA,
          EmbeddingClientTestResource.NVIDIA_URL,
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

  private NvidiaEmbeddingProvider createProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig, int dimension) {
    return new NvidiaEmbeddingProvider(
        PROVIDER_CONFIG, modelConfig, SERVICE_CONFIG, dimension, Map.of());
  }

  private EmbeddingProvider.BatchedEmbeddingResponse runVectorize(
      EmbeddingProvider embeddingProvider,
      List<String> texts,
      EmbeddingProvider.EmbeddingRequestType requestType) {

    return embeddingProvider
        .vectorize(1, texts, testConstants.EMBEDDING_CREDENTIALS, requestType)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem()
        .getItem();
  }

  private static RequestPatternBuilder embeddingRequestFor(String inputText) {
    return postRequestedFor(urlEqualTo(EmbeddingClientTestResource.NVIDIA_PATH))
        .withRequestBody(matchingJsonPath("$.input[0]", equalTo(inputText)));
  }

  private static void assertVectorized(EmbeddingProvider.BatchedEmbeddingResponse response) {
    assertThat(response)
        .isInstanceOf(EmbeddingProvider.BatchedEmbeddingResponse.class)
        .satisfies(
            r -> {
              assertThat(r.embeddings()).isNotNull();
              assertThat(r.embeddings().size()).isEqualTo(1);
              assertThat(r.embeddings().get(0).length).isEqualTo(3);
            });
  }

  @Nested
  class DimensionsParameter {

    @Test
    public void variableDimensionModelSendsDimensionsForIndex() {
      var inputText = MediaType.APPLICATION_JSON + " variable-dimension-index";

      var response =
          runVectorize(
              createProvider(VARIABLE_DIMENSION_MODEL_CONFIG, 512),
              List.of(inputText),
              EmbeddingProvider.EmbeddingRequestType.INDEX);
      assertVectorized(response);

      verify(
          embeddingRequestFor(inputText)
              .withRequestBody(matchingJsonPath("$.model", equalTo(VARIABLE_DIMENSION_MODEL)))
              .withRequestBody(matchingJsonPath("$.input_type", equalTo("passage")))
              .withRequestBody(matchingJsonPath("$.dimensions", equalTo("512"))));
    }

    @Test
    public void variableDimensionModelSendsDimensionsForSearch() {
      var inputText = MediaType.APPLICATION_JSON + " variable-dimension-search";

      var response =
          runVectorize(
              createProvider(VARIABLE_DIMENSION_MODEL_CONFIG, 384),
              List.of(inputText),
              EmbeddingProvider.EmbeddingRequestType.SEARCH);
      assertVectorized(response);

      verify(
          embeddingRequestFor(inputText)
              .withRequestBody(matchingJsonPath("$.input_type", equalTo("query")))
              .withRequestBody(matchingJsonPath("$.dimensions", equalTo("384"))));
    }

    @Test
    public void fixedDimensionModelOmitsDimensions() {
      var inputText = MediaType.APPLICATION_JSON + " fixed-dimension";

      var response =
          runVectorize(
              createProvider(FIXED_DIMENSION_MODEL_CONFIG, 1024),
              List.of(inputText),
              EmbeddingProvider.EmbeddingRequestType.INDEX);
      assertVectorized(response);

      verify(
          embeddingRequestFor(inputText)
              .withRequestBody(matchingJsonPath("$.model", equalTo(FIXED_DIMENSION_MODEL))));
      verify(
          WireMock.exactly(0),
          embeddingRequestFor(inputText).withRequestBody(matchingJsonPath("$.dimensions")));
    }
  }
}
