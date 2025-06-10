package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class OpenAIEmbeddingProvider extends EmbeddingProvider {

  private final OpenAIEmbeddingProviderClient openAIClient;

  public OpenAIEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    // One special case: legacy "ada-002" model does not accept "dimension" parameter
    super(
        ModelProvider.OPENAI,
        requestProperties,
        baseUrl,
        modelName,
        acceptsOpenAIDimensions(modelName) ? dimension : 0,
        vectorizeServiceParameters);

    openAIClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(OpenAIEmbeddingProviderClient.class);
  }

  /**
   * The example response body is:
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
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/error/message";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());

    var openAiRequest =
        new OpenAiEmbeddingRequest(texts.toArray(new String[texts.size()]), modelName(), dimension);
    var organizationId = (String) vectorizeServiceParameters.get("organizationId");
    var projectId = (String) vectorizeServiceParameters.get("projectId");

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    final long callStartNano = System.nanoTime();
    return retryHTTPCall(openAIClient.embed(accessToken, organizationId, projectId, openAiRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var openAiResponse = jakartaResponse.readEntity(OpenAiEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (openAiResponse.data() == null) {
                throw new IllegalStateException(
                    "ModelProvider %s returned empty data for model %s"
                        .formatted(modelProvider(), modelName()));
              }
              Arrays.sort(openAiResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(openAiResponse.data())
                      .map(OpenAiEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.INPUT_TYPE_UNSPECIFIED,
                      openAiResponse.usage().prompt_tokens(),
                      openAiResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);

              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the OpenAI Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface OpenAIEmbeddingProviderClient {
    @POST
    @Path("/embeddings")
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken,
        @HeaderParam("OpenAI-Organization") String organizationId,
        @HeaderParam("OpenAI-Project") String projectId,
        OpenAiEmbeddingRequest request);
  }

  /**
   * Request structure of the OpenAI REST service.
   *
   * <p>..
   */
  public record OpenAiEmbeddingRequest(
      String[] input,
      String model,
      @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  /**
   * Response structure of the OpenAI REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record OpenAiEmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(String object, int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }
}
