package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.EmbeddingProviderErrorMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class HuggingFaceEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.HUGGINGFACE;
  private final HuggingFaceEmbeddingProviderClient huggingFaceEmbeddingProviderClient;

  public HuggingFaceEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    super(requestProperties, baseUrl, model, dimension, vectorizeServiceParameters, providerConfig);

    String actualUrl = replaceParameters(baseUrl, Map.of("modelId", model.name()));

    huggingFaceEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(HuggingFaceEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface HuggingFaceEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<List<float[]>> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return EmbeddingProviderErrorMapper.mapToAPIException(providerId, response, errorMessage);
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
      logger.error(
          "Error response from embedding provider '{}': {}", providerId, rootNode.toString());
      // Extract the "error" node
      JsonNode errorNode = rootNode.path("error");
      // Return the text of the "message" node, or the whole response body if it is missing
      return errorNode.isMissingNode() ? rootNode.toString() : errorNode.toString();
    }
  }

  private record EmbeddingRequest(List<String> inputs, Options options) {
    public record Options(boolean waitForModel) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {
    // Check if using an EOF model
    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());
    EmbeddingRequest request = new EmbeddingRequest(texts, new EmbeddingRequest.Options(true));

    return applyRetry(
            huggingFaceEmbeddingProviderClient.embed(
                HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get(),
                request))
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
