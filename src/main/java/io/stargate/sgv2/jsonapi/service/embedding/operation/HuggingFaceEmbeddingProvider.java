package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;
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
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public HuggingFaceEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(requestProperties, baseUrl, modelName, dimension, vectorizeServiceParameters);

    huggingFaceEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(HuggingFaceEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(NetworkUsageInterceptor.class)
  public interface HuggingFaceEmbeddingProviderClient {
    @POST
    @Path("/{modelId}")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<jakarta.ws.rs.core.Response> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(providerId, response, errorMessage);
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
      logger.info(
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
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());
    EmbeddingRequest request = new EmbeddingRequest(texts, new EmbeddingRequest.Options(true));

    return applyRetry(
            huggingFaceEmbeddingProviderClient.embed(
                "Bearer " + embeddingCredentials.apiKey().get(), modelName, request))
        .onItem()
        .transform(
            resp -> {
              String json = resp.readEntity(String.class); // Read raw JSON
              if (json == null) {
                return new Response(
                    batchId,
                    Collections.emptyList(),
                    new VectorizeUsage(ProviderConstants.HUGGINGFACE, modelName));
              }
              List<float[]> embeddings = null;
              try {
                embeddings = objectMapper.readValue(json, new TypeReference<List<float[]>>() {});
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
              int sentBytes = Integer.parseInt(resp.getHeaderString("sent-bytes"));
              int receivedBytes = Integer.parseInt(resp.getHeaderString("received-bytes"));
              VectorizeUsage vectorizeUsage =
                  new VectorizeUsage(
                      sentBytes, receivedBytes, 0, ProviderConstants.HUGGINGFACE, modelName);
              return new Response(batchId, embeddings, vectorizeUsage);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
