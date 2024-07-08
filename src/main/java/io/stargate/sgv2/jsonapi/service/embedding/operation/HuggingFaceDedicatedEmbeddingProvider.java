package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class HuggingFaceDedicatedEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.HUGGINGFACE_DEDICATED;
  private final HuggingFaceDedicatedEmbeddingProviderClient
      huggingFaceDedicatedEmbeddingProviderClient;

  public HuggingFaceDedicatedEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(requestProperties, baseUrl, modelName, dimension, vectorizeServiceParameters);

    // replace placeholders: endPointName, regionName, cloudName
    String dedicatedApiUrl = replaceParameters(baseUrl, vectorizeServiceParameters);
    huggingFaceDedicatedEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(dedicatedApiUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(HuggingFaceDedicatedEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface HuggingFaceDedicatedEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
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
     *   "message": "Batch size error",
     *   "type": "validation"
     * }
     *
     * {
     *   "message": "Model is overloaded",
     *   "type": "overloaded"
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
      // Extract the "message" node
      JsonNode messageNode = rootNode.path("message");
      // Return the text of the "message" node, or the whole response body if it is missing
      return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
    }
  }

  // huggingfaceDedicated, Test Embeddings Inference, openAI compatible route
  // https://huggingface.github.io/text-embeddings-inference/#/Text%20Embeddings%20Inference/openai_embed
  private record EmbeddingRequest(String[] input) {}

  private record EmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    private record Data(String object, int index, float[] embedding) {}

    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    String[] textArray = new String[texts.size()];
    EmbeddingRequest request = new EmbeddingRequest(texts.toArray(textArray));

    Uni<EmbeddingResponse> response =
        applyRetry(
            huggingFaceDedicatedEmbeddingProviderClient.embed(
                "Bearer " + apiKeyOverride.get(), request));

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
