package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
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

  private final JinaAIEmbeddingProviderClient jinaClient;

  public JinaAIEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.JINA_AI,
        providerConfig,
        modelConfig,
        serviceConfig,
        acceptsJinaAIDimensions(modelConfig.name()) ? dimension : 0,
        vectorizeServiceParameters);

    jinaClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(serviceConfig.getBaseUrl(modelName())))
            .readTimeout(requestProperties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(JinaAIEmbeddingProviderClient.class);
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
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/detail";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());

    var jinaRequest =
        new JinaEmbeddingRequest(
            texts,
            modelName(),
            dimension,
            (String) vectorizeServiceParameters.get("task"),
            (Boolean) vectorizeServiceParameters.get("late_chunking"));

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(jinaClient.embed(accessToken, jinaRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var jinaResponse = decodeResponse(jakartaResponse, JinaEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (jinaResponse.data() == null) {
                throwEmptyData(jakartaResponse);
              }

              Arrays.sort(jinaResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(jinaResponse.data())
                      .map(JinaEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      jinaResponse.usage().prompt_tokens(),
                      jinaResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the Jina Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface JinaAIEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, JinaEmbeddingRequest request);
  }

  /**
   * Request structure of the Voyage REST service.
   *
   * <p>By default, Jina Text Encoding Format is float
   */
  public record JinaEmbeddingRequest(
      List<String> input,
      String model,
      @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions,
      @JsonInclude(value = JsonInclude.Include.NON_NULL) String task,
      @JsonInclude(value = JsonInclude.Include.NON_NULL) Boolean late_chunking) {}

  /**
   * Response structure of the Jina REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record JinaEmbeddingResponse(String object, Data[] data, String model, Usage usage) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Data(String object, int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Usage(int prompt_tokens, int total_tokens) {}
  }
}
