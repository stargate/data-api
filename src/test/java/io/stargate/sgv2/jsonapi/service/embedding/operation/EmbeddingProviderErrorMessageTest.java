package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

/**
 * NOTE: this test relies on the {@link EmbeddingClientTestResource} to mock the server responses
 */
@QuarkusTest
@WithTestResource(EmbeddingClientTestResource.class)
public class EmbeddingProviderErrorMessageTest {

  @Inject EmbeddingProvidersConfig embeddingProvidersConfig;

  private static final int DEFAULT_DIMENSIONS = 0;

  private static final TestConstants testConstants = new TestConstants();

  private final EmbeddingCredentials embeddingCredentials =
      new EmbeddingCredentials(
          testConstants.TENANT, Optional.of("test"), Optional.empty(), Optional.empty());

  private final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig MODEL_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
          "testModel",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          Optional.empty(),
          List.of(),
          Map.of(),
          Optional.empty());

  private final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
              3, 10, 10000, 10000, 0.5, Optional.empty(), Optional.empty(), Optional.empty(), 10);

  private final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl PROVIDER_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
          ModelProvider.NVIDIA.apiName(),
          true,
          Optional.of(
              EmbeddingClientTestResource
                  .NVIDIA_URL), // path important see EmbeddingProviderErrorMessageTest
          false,
          Map.of(),
          List.of(),
          REQUEST_PROPERTIES,
          List.of());

  private final ServiceConfigStore.ServiceConfig SERVICE_CONFIG =
      new ServiceConfigStore.ServiceConfig(
          ModelProvider.NVIDIA,
          EmbeddingClientTestResource
              .NVIDIA_URL, // path important see EmbeddingProviderErrorMessageTest
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

  private io.stargate.sgv2.jsonapi.service.embedding.operation.NvidiaEmbeddingProvider
      createProvider() {
    return new io.stargate.sgv2.jsonapi.service.embedding.operation.NvidiaEmbeddingProvider(
        PROVIDER_CONFIG, MODEL_CONFIG, SERVICE_CONFIG, DEFAULT_DIMENSIONS, null);
  }

  private Throwable vectorizeWithError(String text) {

    return createProvider()
        .vectorize(
            1, List.of(text), embeddingCredentials, EmbeddingProvider.EmbeddingRequestType.INDEX)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .getFailure();
  }

  private void assertApiException(
      Throwable exception, ErrorCode<?> errorCode, String bodyContains) {

    assertThat(exception)
        .asInstanceOf(InstanceOfAssertFactories.type(APIException.class))
        .satisfies(e -> assertThat(e.code).as("ApiException.code").isEqualTo(errorCode.name()))
        .satisfies(e -> assertThat(e.body).as("ApiException.body").contains(bodyContains));
  }

  @Test
  public void test429() throws Exception {

    var exception =
        vectorizeWithError(String.valueOf(Response.Status.TOO_MANY_REQUESTS.getStatusCode()));
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_RATE_LIMITED,
        "Provider 'nvidia' rate limited the request with HTTP 429; error message: {\"object\":\"list\"}");
  }

  @Test
  public void test4xx() throws Exception {

    var exception = vectorizeWithError(String.valueOf(Response.Status.BAD_REQUEST.getStatusCode()));
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_CLIENT_ERROR,
        "Provider 'nvidia' returned a HTTP client error with HTTP 400; error message: {\"object\":\"list\"}");
  }

  @Test
  public void test5xx() throws Exception {

    var exception =
        vectorizeWithError(String.valueOf(Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_SERVER_ERROR,
        "Provider 'nvidia' returned a HTTP server error with HTTP 503; error message: {\"object\":\"list\"}");
  }

  @Test
  public void testRetryError() throws Exception {

    var exception =
        vectorizeWithError(String.valueOf(Response.Status.REQUEST_TIMEOUT.getStatusCode()));
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT,
        "The HTTP status code was: 408.");
  }

  @Test
  public void testCorrectHeaderAndBody() {

    final EmbeddingProvider.BatchedEmbeddingResponse result =
        createProvider()
            .vectorize(
                1,
                List.of(MediaType.APPLICATION_JSON),
                embeddingCredentials,
                EmbeddingProvider.EmbeddingRequestType.INDEX)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem(Duration.ofDays(1))
            .getItem();

    assertThat(result).isNotNull();
    assertThat(result.batchId()).isEqualTo(1);
    assertThat(result.embeddings()).isNotNull();
  }

  @Test
  public void testIncorrectContentTypeXML() {

    var exception = vectorizeWithError("application/xml");
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE,
        "Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found 'application/xml'; HTTP Status: 200; The response body is: '<object>list</object>'.");
  }

  @Test
  public void testIncorrectContentTypePlainText() {

    var exception = vectorizeWithError("text/plain;charset=UTF-8");
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE,
        "Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found 'text/plain;charset=UTF-8'; HTTP Status: 200; The response body is: 'vectors as plain text'.");
  }

  @Test
  public void testNoJsonResponse() {

    var exception = vectorizeWithError("no json body");
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE,
        "No response body from the embedding provider");
  }

  @Test
  public void testEmptyJsonResponse() {

    var exception = vectorizeWithError("empty json body");
    assertApiException(
        exception,
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE,
        "Provider: nvidia; HTTP Status: 200; Error Message: The embedding provider returned empty data for model testModel");
  }
}
