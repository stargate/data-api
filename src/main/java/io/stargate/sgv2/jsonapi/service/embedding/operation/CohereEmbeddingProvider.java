package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import jakarta.ws.rs.Path;
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

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen Cohere model.
 */
public class CohereEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.COHERE;
  private final CohereEmbeddingProviderClient cohereEmbeddingProviderClient;

  public CohereEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    super(requestProperties, baseUrl, model, dimension, vectorizeServiceParameters, providerConfig);

    cohereEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(CohereEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface CohereEmbeddingProviderClient {
    @POST
    @Path("/embed")
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return EmbeddingProviderErrorMapper.mapToAPIException(providerId, response, errorMessage);
    }

    /**
     * Extract the error message from the response body. The example response body is:
     *
     * <pre>
     * {
     *   "message": "invalid api token"
     * }
     *
     * 429 response body:
     * {
     *   "data": "string"
     * }
     * </pre>
     *
     * @param response The response body as a String.
     * @return The error message extracted from the response body.
     */
    private static String getErrorMessage(jakarta.ws.rs.core.Response response) {
      // Get the whole response body
      JsonNode rootNode = response.readEntity(JsonNode.class);
      // Log the response body
      logger.error(
          "Error response from embedding provider '{}': {}", providerId, rootNode.toString());
      // Check if the root node contains a "message" field
      JsonNode messageNode = rootNode.path("message");
      if (!messageNode.isMissingNode()) {
        return messageNode.toString();
      }
      // Check if the root node contains a "data" field
      JsonNode dataNode = rootNode.path("data");
      if (!dataNode.isMissingNode()) {
        return dataNode.toString();
      }
      // Return the whole response body if no message or data field is found
      return rootNode.toString();
    }
  }

  private record EmbeddingRequest(String[] texts, String model, String input_type) {}

  // @JsonIgnoreProperties({"id", "texts", "meta", "response_type"})
  @JsonIgnoreProperties(ignoreUnknown = true) // ignore possible extra fields without error
  private static class EmbeddingResponse {

    protected EmbeddingResponse() {}

    private List<float[]> embeddings;

    public List<float[]> getEmbeddings() {
      return embeddings;
    }

    public void setEmbeddings(List<float[]> embeddings) {
      this.embeddings = embeddings;
    }
  }

  // Input type to be used for vector search should "search_query"
  private static final String SEARCH_QUERY = "search_query";
  private static final String SEARCH_DOCUMENT = "search_document";

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {
    // Check if using an EOF model
    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());

    String[] textArray = new String[texts.size()];
    String input_type =
        embeddingRequestType == EmbeddingRequestType.INDEX ? SEARCH_DOCUMENT : SEARCH_QUERY;
    EmbeddingRequest request =
        new EmbeddingRequest(texts.toArray(textArray), model.name(), input_type);

    Uni<EmbeddingResponse> response =
        applyRetry(
            cohereEmbeddingProviderClient.embed(
                HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get(),
                request));

    return response
        .onItem()
        .transform(
            resp -> {
              if (resp.getEmbeddings() == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              return Response.of(batchId, resp.getEmbeddings());
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
