package io.stargate.sgv2.jsonapi.service.embedding.operation;

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
import jakarta.ws.rs.core.*;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class HuggingFaceEmbeddingProvider extends EmbeddingProvider {

  private final HuggingFaceEmbeddingProviderClient huggingFaceClient;

  public HuggingFaceEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.HUGGINGFACE,
        providerConfig,
        modelConfig,
        serviceConfig,
        dimension,
        vectorizeServiceParameters);

    var baseUrl = serviceConfig.getBaseUrl(modelName());
    // replace was added in https://github.com/stargate/data-api/pull/2108/files
    var actualUrl = replaceParameters(baseUrl, Map.of("modelId", modelName()));

    huggingFaceClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(requestProperties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(HuggingFaceEmbeddingProviderClient.class);
  }

  /**
   * The example response body is:
   *
   * <pre>
   * {
   *   "error": "Authorization header is correct, but the token seems invalid"
   * }
   * </pre>
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/error";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());
    var huggingFaceRequest =
        new HuggingFaceEmbeddingRequest(texts, new HuggingFaceEmbeddingRequest.Options(true));

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(huggingFaceClient.embed(accessToken, huggingFaceRequest))
        .onItem()
        .transform(
            jakartaResponse -> {

              // NOTE: Boxing happening here, as the response is a JSON array of arrays of floats.
              // should return zero legnth list if entity is null or empty.
              // TODO: how to deserialise without boxing ?
              List<Float[]> vectorsBoxed = jakartaResponse.readEntity(new GenericType<>() {});
              long callDurationNano = System.nanoTime() - callStartNano;

              List<float[]> vectorsUnboxed =
                  vectorsBoxed.stream()
                      .map(
                          vector -> {
                            if (vector == null) {
                              return new float[0]; // Handle null vectors
                            }
                            float[] unboxed = new float[vector.length];
                            for (int i = 0; i < vector.length; i++) {
                              unboxed[i] = vector[i];
                            }
                            return unboxed;
                          })
                      .toList();

              // The hugging face API we are calling does not return usage information, there may be
              // a
              // newer version of the API that does, but for now we will not return usage
              // information.
              // https://huggingface.co/blog/getting-started-with-embeddings
              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenant(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      0,
                      0,
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectorsUnboxed, modelUsage);
            });
  }

  /**
   * REST client interface for the HuggingFace Embedding Service.
   *
   * <p>.. NOTE: the response is just a JSON array of arrays of floats, e.g.:
   *
   * <pre>
   *   [[-0.123, 0.456, ...], [-0.789, 0.012, ...], ...]
   * </pre>
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface HuggingFaceEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, HuggingFaceEmbeddingRequest request);
  }

  /**
   * Request structure of the HuggingFace REST service.
   *
   * <p>..
   */
  public record HuggingFaceEmbeddingRequest(List<String> inputs, Options options) {
    public record Options(boolean waitForModel) {}
  }
}
