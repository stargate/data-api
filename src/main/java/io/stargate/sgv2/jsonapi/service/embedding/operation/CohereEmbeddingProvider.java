package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ProviderHttpInterceptor;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
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

  private final CohereEmbeddingProviderClient cohereClient;

  public CohereEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.COHERE,
        providerConfig,
        modelConfig,
        serviceConfig,
        dimension,
        vectorizeServiceParameters);

    cohereClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(serviceConfig.getBaseUrl(modelName())))
            .readTimeout(requestProperties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(CohereEmbeddingProviderClient.class);
  }

  @Override
  protected String errorMessageJsonPtr() {
    // overriding the function that calls this
    return "";
  }

  /**
   * The example response body is:
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
   */
  @Override
  protected String responseErrorMessage(JsonNode rootNode) {

    JsonNode messageNode = rootNode.path("message");
    if (!messageNode.isMissingNode()) {
      return messageNode.toString();
    }

    JsonNode dataNode = rootNode.path("data");
    if (!dataNode.isMissingNode()) {
      return dataNode.toString();
    }

    // Return the whole response body if no message or data field is found
    return rootNode.toString();
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());

    // Input type to be used for vector search should "search_query"
    var input_type =
        embeddingRequestType == EmbeddingRequestType.INDEX ? "search_document" : "search_query";
    var cohereRequest =
        new CohereEmbeddingRequest(
            texts.toArray(new String[texts.size()]), modelName(), input_type);

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();

    return retryHTTPCall(cohereClient.embed(accessToken, cohereRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var cohereResponse = decodeResponse(jakartaResponse, CohereEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (cohereResponse.embeddings() == null) {
                throwEmptyData(jakartaResponse);
              }

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      cohereResponse.meta().billed_units().input_tokens(),
                      cohereResponse.meta().billed_units().input_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(
                  batchId, cohereResponse.embeddings().values(), modelUsage);
            });
  }

  /**
   * REST client interface for the Cohere Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface CohereEmbeddingProviderClient {
    @POST
    @Path("/embed")
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, CohereEmbeddingRequest request);
  }

  /**
   * Request structure of the Cohere REST service.
   *
   * <p>..
   */
  public record CohereEmbeddingRequest(String[] texts, String model, String input_type) {}

  /**
   * Response structure of the Cohere REST service.
   *
   * <p>aaron - 9 June 2025, change from class to record, check git if this breaks.
   * https://docs.cohere.com/reference/embed#response
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record CohereEmbeddingResponse(
      String id, List<String> texts, Embeddings embeddings, Meta meta) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Embeddings(@JsonProperty("float") List<float[]> values) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Meta(ApiVersion api_version, BilledUnits billed_units, List<String> warnings) {
      @JsonIgnoreProperties(ignoreUnknown = true)
      public record ApiVersion(String version, boolean is_experimental) {}

      @JsonIgnoreProperties(ignoreUnknown = true)
      public record BilledUnits(int input_tokens) {}
    }
  }
}
