package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class HuggingFaceEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String modelName;
  private String baseUrl;
  private final HuggingFaceEmbeddingProvider embeddingProvider;
  private Map<String, Object> vectorizeServiceParameters;

  public HuggingFaceEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    this.vectorizeServiceParameters = vectorizeServiceParameters;
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(HuggingFaceEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface HuggingFaceEmbeddingProvider {
    @POST
    @Path("/{modelId}")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<List<float[]>> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(
          ProviderConstants.HUGGINGFACE, response, errorMessage);
    }

    /**
     * Extracts the error message from the response body. The example response body is:
     *
     * <pre>
     * {
     *   "error": "Authorization header is correct, but the token seems invalid"
     * }
     * </pre>
     *
     * @param response The response body as a String.
     * @return The error message extracted from the response body, or null if the message is not
     *     found.
     */
    private static String getErrorMessage(jakarta.ws.rs.core.Response response) {
      // Get the whole response body
      JsonNode rootNode = response.readEntity(JsonNode.class);
      // Log the response body
      logger.info(String.format("Error response from embedding provider: %s", rootNode.toString()));
      // Extract the "error" node
      JsonNode errorNode = rootNode.path("error");
      // Return the text of the "message" node, or the whole response body if it is missing
      return errorNode.isMissingNode() ? rootNode.asText() : errorNode.asText();
    }
  }

  private record EmbeddingRequest(List<String> inputs, Options options) {
    public record Options(boolean waitForModel) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    EmbeddingRequest request = new EmbeddingRequest(texts, new EmbeddingRequest.Options(true));
    return embeddingProvider
        .embed("Bearer " + apiKeyOverride.get(), modelName, request)
        .onFailure(
            throwable -> {
              return ((throwable.getCause() != null
                      && throwable.getCause() instanceof JsonApiException jae
                      && jae.getErrorCode() == ErrorCode.EMBEDDING_PROVIDER_TIMEOUT)
                  || throwable instanceof TimeoutException);
            })
        .retry()
        .withBackOff(
            Duration.ofMillis(requestProperties.initialBackOffMillis()),
            Duration.ofMillis(requestProperties.maxBackOffMillis()))
        .withJitter(requestProperties.jitter())
        .atMost(requestProperties.atMostRetries())
        .onItem()
        .transform(
            resp -> {
              if (resp == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              return Response.of(batchId, resp);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
