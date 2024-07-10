package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class UpstageAIEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.UPSTAGE_AI;
  private static final String UPSTAGE_MODEL_SUFFIX_QUERY = "-query";
  private static final String UPSTAGE_MODEL_SUFFIX_PASSAGE = "-passage";
  private final String modelNamePrefix;
  private final UpstageAIEmbeddingProviderClient upstageAIEmbeddingProviderClient;

  public UpstageAIEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelNamePrefix,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(requestProperties, baseUrl, modelNamePrefix, dimension, vectorizeServiceParameters);

    this.modelNamePrefix = modelNamePrefix;
    upstageAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(UpstageAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface UpstageAIEmbeddingProviderClient {
    @POST
    // no path specified, as it is already included in the baseUri
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

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
          String.format(
              "Error response from embedding provider '%s': %s", providerId, rootNode.toString()));
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
  }

  // NOTE: "input" is a single String, not array of Strings!
  record EmbeddingRequest(String input, String model) {}

  @JsonIgnoreProperties({"object"})
  record EmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties({"object"})
    record Data(int index, float[] embedding) {}

    record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKey,
      EmbeddingRequestType embeddingRequestType) {
    checkEmbeddingApiKeyHeader(providerId, apiKey);
    // Oddity: Implementation does not support batching, so we only accept "batches"
    // of 1 String, fail for others
    if (texts.size() != 1) {
      // Temporary fail message: with re-batching will give better information
      throw ErrorCode.INVALID_VECTORIZE_VALUE_TYPE.toApiException(
          "UpstageAI only supports vectorization of 1 text at a time, got " + texts.size());
    }
    // Another oddity: model name used as prefix
    final String modelName =
        modelNamePrefix
            + ((embeddingRequestType == EmbeddingRequestType.SEARCH)
                ? UPSTAGE_MODEL_SUFFIX_QUERY
                : UPSTAGE_MODEL_SUFFIX_PASSAGE);

    EmbeddingRequest request = new EmbeddingRequest(texts.get(0), modelName);

    Uni<EmbeddingResponse> response =
        applyRetry(upstageAIEmbeddingProviderClient.embed("Bearer " + apiKey.get(), request));

    return response
        .onItem()
        .transform(
            resp -> {
              if (resp.data() == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(resp.data()).map(data -> data.embedding()).toList();
              return Response.of(batchId, vectors);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
