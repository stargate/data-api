package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ProviderHttpInterceptor;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class VertexAIEmbeddingProvider extends EmbeddingProvider {

  private final VertexAIEmbeddingProviderClient vertexClient;

  public VertexAIEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.VERTEXAI,
        providerConfig,
        baseUrl,
        modelConfig,
        dimension,
        vectorizeServiceParameters);

    String actualUrl = replaceParameters(baseUrl, vectorizeServiceParameters);
    vertexClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(providerConfig.properties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(VertexAIEmbeddingProviderClient.class);
  }

  @Override
  protected String errorMessageJsonPtr() {
    // overriding the call that needs this.
    return null;
  }

  @Override
  protected String responseErrorMessage(Response jakartaResponse) {
    // aaron 9 june 2025 - this is what it did originally, just get the whole response body

    // Get the whole response body
    JsonNode rootNode = jakartaResponse.readEntity(JsonNode.class);
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

    var vertexRequest =
        new VertexEmbeddingRequest(
            texts.stream().map(VertexEmbeddingRequest.Content::new).toList());

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(vertexClient.embed(accessToken, modelName(), vertexRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var vertexResponse = jakartaResponse.readEntity(VertexEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (vertexResponse.predictions() == null) {
                throw new IllegalStateException(
                    "ModelProvider %s returned empty data for model %s"
                        .formatted(modelProvider(), modelName()));
              }

              // token usage is for each of the embeddings , need to sum it up
              int total_tokens = 0;
              List<float[]> vectors = new ArrayList<>(vertexResponse.predictions().size());
              for (var prediction : vertexResponse.predictions()) {
                vectors.add(prediction.embeddings().values);
                total_tokens += prediction.embeddings().statistics().token_count;
              }

              // Docs say the token_count in the response is the "Number of tokens of the input
              // text."
              // so seems safe ot use this as the prompt_tokens and total_tokens
              // https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api#response_body
              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      total_tokens,
                      total_tokens,
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the Vertex Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface VertexAIEmbeddingProviderClient {

    @POST
    @Path("/{modelId}:predict")
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        VertexEmbeddingRequest request);
  }

  /**
   * Request structure of the Vertex REST service.
   *
   * <p>..
   */
  private record VertexEmbeddingRequest(List<Content> instances) {
    public record Content(String content) {}
  }

  /**
   * Response structure of the Vertex REST service.
   *
   * <p>.. aaron - 10 June 2025 - this used to be a class, moved to be a record for consistency
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record VertexEmbeddingResponse(
      List<Prediction> predictions,
      // aaron 10 june 2025, could not see metadata in API docs, but it was in the old code.
      // https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api#response_body
      @JsonIgnore Object metadata) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Prediction(Embeddings embeddings) {

      @JsonIgnoreProperties(ignoreUnknown = true)
      public record Embeddings(float[] values, Statistics statistics) {

        @JsonIgnoreProperties(ignoreUnknown = true)
        public record Statistics(boolean truncated, int token_count) {}
      }
    }
  }
}
