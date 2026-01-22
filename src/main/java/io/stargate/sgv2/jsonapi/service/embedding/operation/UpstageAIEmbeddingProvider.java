package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ProviderHttpInterceptor;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class UpstageAIEmbeddingProvider extends EmbeddingProvider {

  private static final String UPSTAGE_MODEL_SUFFIX_QUERY = "-query";
  private static final String UPSTAGE_MODEL_SUFFIX_PASSAGE = "-passage";

  private final String modelNamePrefix;
  private final UpstageAIEmbeddingProviderClient upstageClient;

  public UpstageAIEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.UPSTAGE_AI,
        providerConfig,
        modelConfig,
        serviceConfig,
        dimension,
        vectorizeServiceParameters);

    this.modelNamePrefix = modelConfig.name();
    upstageClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(serviceConfig.getBaseUrl(modelName())))
            .readTimeout(requestProperties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(UpstageAIEmbeddingProviderClient.class);
  }

  @Override
  protected String errorMessageJsonPtr() {
    // overriding the function that calls this
    return "";
  }

  /**
   * Extracts the error message from the response body. The example response body is:
   *
   * <pre>
   * {
   *   "message": "Unauthorized"
   * }
   *
   * {
   *   "error": {
   *     "message": "This model's maximum context length is 4000 tokens. however you requested 10969 tokens. Please reduce your prompt.",
   *     "type": "invalid_request_error",
   *     "param": null,
   *     "code": null
   *   }
   * }
   * </pre>
   */
  @Override
  protected String responseErrorMessage(Response jakartaResponse) {

    JsonNode rootNode = jakartaResponse.readEntity(JsonNode.class);

    // Check if the root node contains a "message" field
    JsonNode messageNode = rootNode.path("message");
    if (!messageNode.isMissingNode()) {
      return messageNode.asText();
    }

    // If the "message" field is not found, check for the nested "error" object
    JsonNode errorMessageNode = rootNode.at("/error/message");
    if (!errorMessageNode.isMissingNode()) {
      return errorMessageNode.asText();
    }
    // Return the whole response body if no message is found
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

    // Oddity: Implementation does not support batching, so we only accept "batches"
    // of 1 String, fail for others
    if (texts.size() != 1) {
      // TODO: This should be IllegalArgumentException

      // Temporary fail message: with re-batching will give better information
      throw ErrorCodeV1.INVALID_VECTORIZE_VALUE_TYPE.toApiException(
          "UpstageAI only supports vectorization of 1 text at a time, got " + texts.size());
    }

    // Another oddity: model name used as prefix
    final String modelName =
        modelNamePrefix
            + ((embeddingRequestType == EmbeddingRequestType.SEARCH)
                ? UPSTAGE_MODEL_SUFFIX_QUERY
                : UPSTAGE_MODEL_SUFFIX_PASSAGE);

    var upstageRequest = new UpstageEmbeddingRequest(texts.getFirst(), modelName);

    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(upstageClient.embed(accessToken, upstageRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var upstageResponse = decodeResponse(jakartaResponse, UpstageEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (upstageResponse.data() == null) {
                throwEmptyData(jakartaResponse);
              }

              // aaron - 11 june 2025 - prev code would sort upstageResponse.data() BUT per above we
              // only support a batch size of 1, so no need to sort.

              List<float[]> vectors =
                  Arrays.stream(upstageResponse.data())
                      .map(UpstageEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenant(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      upstageResponse.usage().prompt_tokens(),
                      upstageResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the Upstage Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface UpstageAIEmbeddingProviderClient {
    @POST
    // no path specified, as it is already included in the baseUri
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, UpstageEmbeddingRequest request);
  }

  /**
   * Request structure of the Upstage REST service.
   *
   * <p>NOTE: "input" is a single String, not array of Constants!
   */
  public record UpstageEmbeddingRequest(String input, String model) {}

  /**
   * Response structure of the Upstage REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record UpstageEmbeddingResponse(Data[] data, String model, Usage usage) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Data(int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Usage(int prompt_tokens, int total_tokens) {}
  }
}
