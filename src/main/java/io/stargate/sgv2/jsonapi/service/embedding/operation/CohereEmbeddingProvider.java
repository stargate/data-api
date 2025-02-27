package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
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
import jakarta.ws.rs.core.Response;
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
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(requestProperties, baseUrl, modelName, dimension, vectorizeServiceParameters);

    cohereEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(CohereEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(NetworkUsageInterceptor.class)
  public interface CohereEmbeddingProviderClient {
    @POST
    @Path("/embed")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<jakarta.ws.rs.core.Response> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(providerId, response, errorMessage);
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
      logger.info(
          String.format(
              "Error response from embedding provider '%s': %s", providerId, rootNode.toString()));
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

  @JsonIgnoreProperties({"id", "texts", "meta", "response_type"})
  private static class EmbeddingResponse {

    protected EmbeddingResponse() {}

    private List<float[]> embeddings;

    private BilledUnits billed_units;

    public List<float[]> getEmbeddings() {
      return embeddings;
    }

    public void setEmbeddings(List<float[]> embeddings) {
      this.embeddings = embeddings;
    }

    public BilledUnits getBilled_units() {
      return billed_units;
    }

    public void setBilled_units(BilledUnits billed_units) {
      this.billed_units = billed_units;
    }

    private static class BilledUnits {
      public int input_tokens;

      public BilledUnits() {}

      public int getInput_tokens() {
        return input_tokens;
      }

      public void setInput_tokens(int input_tokens) {
        this.input_tokens = input_tokens;
      }
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
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());

    String[] textArray = new String[texts.size()];
    String input_type =
        embeddingRequestType == EmbeddingRequestType.INDEX ? SEARCH_DOCUMENT : SEARCH_QUERY;
    EmbeddingRequest request =
        new EmbeddingRequest(texts.toArray(textArray), modelName, input_type);

    Uni<jakarta.ws.rs.core.Response> response =
        applyRetry(
            cohereEmbeddingProviderClient.embed(
                "Bearer " + embeddingCredentials.apiKey().get(), request));

    return response
        .onItem()
        .transform(
            resp -> {
              EmbeddingResponse embeddingResponse = resp.readEntity(EmbeddingResponse.class);
              if (embeddingResponse.getEmbeddings() == null) {
                return new Response(
                    batchId,
                    Collections.emptyList(),
                    new VectorizeUsage(ProviderConstants.COHERE, modelName));
              }
              int sentBytes = Integer.parseInt(resp.getHeaderString("sent-bytes"));
              int receivedBytes = Integer.parseInt(resp.getHeaderString("received-bytes"));
              VectorizeUsage vectorizeUsage =
                  new VectorizeUsage(
                      sentBytes,
                      receivedBytes,
                      embeddingResponse.getBilled_units().getInput_tokens(),
                      ProviderConstants.COHERE,
                      modelName);
              return new Response(batchId, embeddingResponse.getEmbeddings(), vectorizeUsage);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
