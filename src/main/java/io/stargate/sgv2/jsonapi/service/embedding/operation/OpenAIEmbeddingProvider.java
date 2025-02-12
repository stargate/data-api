package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@ActivateRequestContext
public class OpenAIEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.OPENAI;
  private final OpenAIEmbeddingProviderClient openAIEmbeddingProviderClient;

  public OpenAIEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    // One special case: legacy "ada-002" model does not accept "dimension" parameter
    super(
        requestProperties,
        baseUrl,
        modelName,
        acceptsOpenAIDimensions(modelName) ? dimension : 0,
        vectorizeServiceParameters);

    openAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(OpenAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(NetworkUsageInterceptor.class)
  public interface OpenAIEmbeddingProviderClient {
    @POST
    @Path("/embeddings")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<jakarta.ws.rs.core.Response> embed(
        @HeaderParam("Authorization") String accessToken,
        @HeaderParam("OpenAI-Organization") String organizationId,
        @HeaderParam("OpenAI-Project") String projectId,
        EmbeddingRequest request);

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
     *   "error": {
     *     "message": "You exceeded your current quota, please check your plan and billing details. For
     *                 more information on this error, read the docs:
     *                 https://platform.openai.com/docs/guides/error-codes/api-errors.",
     *     "type": "insufficient_quota",
     *     "param": null,
     *     "code": "insufficient_quota"
     *   }
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
          "Error response from embedding provider '{}': {}", providerId, rootNode.toString());
      // Extract the "message" node from the "error" node
      JsonNode messageNode = rootNode.at("/error/message");
      // Return the text of the "message" node, or the whole response body if it is missing
      return messageNode.isMissingNode() ? rootNode.toString() : messageNode.asText();
    }
  }

  private record EmbeddingRequest(
      String[] input,
      String model,
      @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  private record EmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    private record Data(String object, int index, float[] embedding) {}

    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());
    String[] textArray = new String[texts.size()];
    EmbeddingRequest request = new EmbeddingRequest(texts.toArray(textArray), modelName, dimension);
    String organizationId = (String) vectorizeServiceParameters.get("organizationId");
    String projectId = (String) vectorizeServiceParameters.get("projectId");

    // ✅ Create an instance of NetworkUsageInfo and pass it to request properties
    Uni<jakarta.ws.rs.core.Response> response =
        applyRetry(
            openAIEmbeddingProviderClient.embed(
                "Bearer " + embeddingCredentials.apiKey().get(),
                organizationId,
                projectId,
                request)); // Pass the object dynamically

    return response
        .onItem()
        .transform(
            res -> {
              EmbeddingResponse embeddingResponse = res.readEntity(EmbeddingResponse.class);
              if (embeddingResponse.data() == null) {
                return new Response(
                    batchId,
                    Collections.emptyList(),
                    new VectorizeUsage(ProviderConstants.OPENAI, modelName));
              }
              int sentBytes = Integer.parseInt(res.getHeaderString("sent-bytes"));
              int receivedBytes = Integer.parseInt(res.getHeaderString("received-bytes"));
              VectorizeUsage vectorizeUsage =
                  new VectorizeUsage(
                      sentBytes,
                      receivedBytes,
                      embeddingResponse.usage().total_tokens(),
                      ProviderConstants.OPENAI,
                      modelName);
              Arrays.sort(embeddingResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(embeddingResponse.data()).map(data -> data.embedding()).toList();
              return new Response(batchId, vectors, vectorizeUsage);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
