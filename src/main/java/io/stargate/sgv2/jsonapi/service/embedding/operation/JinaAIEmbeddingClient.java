package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Implementation of client that talks to jina.ai embedding provider. See <a
 * href="https://api.jina.ai/redoc#tag/embeddings">API reference</a> for details of REST API being
 * called.
 */
public class JinaAIEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String modelName;
  private final JinaAIEmbeddingProvider embeddingProvider;

  public JinaAIEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.modelName = modelName;
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(JinaAIEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface JinaAIEmbeddingProvider {
    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(
          ProviderConstants.JINA_AI, response, errorMessage);
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
      String responseBody = response.readEntity(String.class);
      try {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(responseBody);
        // Extract the "detail" node
        JsonNode detailNode = rootNode.path("detail");
        // Assuming the error message is within the "msg" part of the detail
        if (!detailNode.isMissingNode()) {
          String detailText = detailNode.asText();
          // Use regex to extract the message part from the detail text
          java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("'msg':\\s*'([^']*)'");
          java.util.regex.Matcher matcher = pattern.matcher(detailText);
          if (matcher.find()) {
            return matcher.group(1);
          }
          // If the regex fails, return the whole detail text
            return detailText;
        }
        // Return the whole response body if no "detail" is found
        return responseBody;
      } catch (Exception e) {
        // should not go here, already check json format in EmbeddingProviderResponseValidation.
        // if it happens, return the whole response body
        return responseBody;
      }
    }
  }

  // By default, Jina Text Encoding Format is float
  private record EmbeddingRequest(List<String> input, String model) {}

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
    EmbeddingRequest request = new EmbeddingRequest(texts, modelName);
    Uni<EmbeddingResponse> response =
        embeddingProvider
            .embed("Bearer " + apiKeyOverride.get(), request)
            .onFailure(
                throwable -> {
                  return ((throwable.getCause() != null
                          && throwable.getCause() instanceof JsonApiException jae
                          && jae.getErrorCode() == ErrorCode.EMBEDDING_PROVIDER_TIMEOUT)
                      || throwable instanceof TimeoutException);
                })
            .retry()
            // Jina has intermittent embedding failure, set a longer retry delay to mitigate the
            // issue
            .withBackOff(
                Duration.ofMillis(requestProperties.initialBackOffMillis()),
                Duration.ofMillis(requestProperties.maxBackOffMillis()))
            .withJitter(requestProperties.jitter())
            .atMost(requestProperties.atMostRetries());
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
