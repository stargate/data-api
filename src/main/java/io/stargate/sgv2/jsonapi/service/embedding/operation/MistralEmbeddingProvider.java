package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
 * Implementation of client that talks to Mistral embedding provider. See <a
 * href="https://docs.mistral.ai/api/#operation/createEmbedding">API reference</a> for details of
 * REST API being called.
 */
public class MistralEmbeddingProvider extends EmbeddingProvider {

  private final MistralEmbeddingProviderClient mistralClient;

  public MistralEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.MISTRAL,
        providerConfig,
        modelConfig,
        serviceConfig,
        dimension,
        vectorizeServiceParameters);

    mistralClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(serviceConfig.getBaseUrl(modelName())))
            .readTimeout(requestProperties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(MistralEmbeddingProviderClient.class);
  }

  /**
   * The example response body is:
   *
   * <pre>
   * {
   *   "message":"Unauthorized",
   *   "request_id":"1383ed1b472cb85fdfaa9624515d2d0e"
   * }
   *
   * {
   *   "object":"error",
   *   "message":"Input is too long. Max length is 8192 got 10970",
   *   "type":"invalid_request_error",
   *   "param":null,
   *   "code":null
   * }
   * </pre>
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/message";
  }

  /**
   * Mistral for 401 Unauthorized returns a response with no content type and just the text
   * "Unauthorized".
   */
  @Override
  protected String responseErrorMessage(Response jakartaResponse) {
    if (jakartaResponse.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
      return Response.Status.UNAUTHORIZED.getReasonPhrase();
    }
    return super.responseErrorMessage(jakartaResponse);
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());

    var mistralRequest = new MistralEmbeddingRequest(texts, modelName(), "float");
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();

    return retryHTTPCall(mistralClient.embed(accessToken, mistralRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var mistralResponse = decodeResponse(jakartaResponse, MistralEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (mistralResponse.data() == null) {
                throwEmptyData(jakartaResponse);
              }

              Arrays.sort(mistralResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(mistralResponse.data())
                      .map(MistralEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenant(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      mistralResponse.usage().prompt_tokens(),
                      mistralResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the Mistral Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface MistralEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, MistralEmbeddingRequest request);
  }

  /**
   * Request structure of the Mistral REST service.
   *
   * <p>..
   */
  public record MistralEmbeddingRequest(List<String> input, String model, String encoding_format) {}

  /**
   * Response structure of the Mistral REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record MistralEmbeddingResponse(
      String id, String object, Data[] data, String model, Usage usage) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(String object, int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(
        int prompt_tokens, int total_tokens, int completion_tokens, int request_count) {}
  }
}
