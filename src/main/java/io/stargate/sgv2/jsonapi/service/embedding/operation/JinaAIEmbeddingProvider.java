package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Implementation of client that talks to jina.ai embedding provider. See <a
 * href="https://api.jina.ai/redoc#tag/embeddings">API reference</a> for details of REST API being
 * called.
 */
public class JinaAIEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.JINA_AI;
  private final JinaAIEmbeddingProviderClient jinaAIEmbeddingProviderClient;

  public JinaAIEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    super(
        requestProperties,
        baseUrl,
        model,
        acceptsJinaAIDimensions(model.name()) ? dimension : 0,
        vectorizeServiceParameters,
        providerConfig);

    jinaAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(JinaAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface JinaAIEmbeddingProviderClient {
    @POST
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
     *    "detail": "ValidationError(model='TextDoc', errors=[{'loc': ('text',), 'msg': 'Single text cannot exceed 8192 tokens. 10454 tokens given.', 'type': 'value_error'}])"
     * }
     * </pre>
     *
     * <pre>
     *     {"detail":"Failed to authenticate with the provided api key."}
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
      // Extract the "detail" node
      JsonNode detailNode = rootNode.path("detail");
      return detailNode.isMissingNode() ? rootNode.toString() : detailNode.toString();
    }
  }

  // By default, Jina Text Encoding Format is float
  private record EmbeddingRequest(
      List<String> input,
      String model,
      @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions,
      @JsonInclude(value = JsonInclude.Include.NON_NULL) String task,
      @JsonInclude(value = JsonInclude.Include.NON_NULL) Boolean late_chunking) {}

  @JsonIgnoreProperties(ignoreUnknown = true) // ignore possible extra fields without error
  private record EmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(String object, int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
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

    EmbeddingRequest request =
        new EmbeddingRequest(
            texts,
            model.name(),
            dimension,
            (String) vectorizeServiceParameters.get("task"),
            (Boolean) vectorizeServiceParameters.get("late_chunking"));

    Uni<EmbeddingResponse> response =
        applyRetry(
            jinaAIEmbeddingProviderClient.embed(
                HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get(),
                request));

    return response
        .onItem()
        .transform(
            resp -> {
              if (resp.data() == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(resp.data()).map(EmbeddingResponse.Data::embedding).toList();
              return Response.of(batchId, vectors);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
