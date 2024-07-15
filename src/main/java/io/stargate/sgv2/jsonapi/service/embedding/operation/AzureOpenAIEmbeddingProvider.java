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
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Implementation of client that talks to Azure-deployed OpenAI embedding provider. See <a
 * href="https://learn.microsoft.com/en-us/azure/ai-services/openai/reference">API reference</a> for
 * details of REST API being called.
 */
public class AzureOpenAIEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.AZURE_OPENAI;
  private final OpenAIEmbeddingProviderClient openAIEmbeddingProviderClient;

  public AzureOpenAIEmbeddingProvider(
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

    String actualUrl = replaceParameters(baseUrl, vectorizeServiceParameters);
    openAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(OpenAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface OpenAIEmbeddingProviderClient {
    @POST
    // no path specified, as it is already included in the baseUri
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        // API keys as "api-key", MS Entra as "Authorization: Bearer [token]
        @HeaderParam("api-key") String accessToken, EmbeddingRequest request);

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
     *     "code": "401",
     *     "message": "Access denied due to invalid subscription key or wrong API endpoint. Make sure to provide a valid key for an active subscription and use a correct regional API endpoint for your resource."
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
          String.format(
              "Error response from embedding provider '%s': %s", providerId, rootNode.toString()));
      // Extract the "message" node from the "error" node
      JsonNode messageNode = rootNode.at("/error/message");
      // Return the text of the "message" node, or the whole response body if it is missing
      return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
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

    // NOTE: NO "Bearer " prefix with API key for Azure OpenAI
    Uni<EmbeddingResponse> response =
        applyRetry(
            openAIEmbeddingProviderClient.embed(embeddingCredentials.apiKey().get(), request));

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
