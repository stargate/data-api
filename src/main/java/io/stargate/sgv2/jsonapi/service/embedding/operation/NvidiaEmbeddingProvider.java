package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen Nvidia model.
 */
public class NvidiaEmbeddingProvider extends EmbeddingProvider {

  private final NvidiaEmbeddingProviderClient nvidiaClient;

  public NvidiaEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.NVIDIA,
        providerConfig,
        baseUrl,
        modelConfig,
        dimension,
        vectorizeServiceParameters);

    nvidiaClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(providerConfig.properties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(NvidiaEmbeddingProviderClient.class);
  }

  /**
   * The example response body is:
   *
   * <pre>
   * {
   *   "object": "error",
   *   "message": "Input length exceeds the maximum token length of the model",
   *   "detail": {},
   *   "type": "invalid_request_error"
   * }
   * </pre>
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/message";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();
    var input_type = embeddingRequestType == EmbeddingRequestType.INDEX ? "passage" : "query";
    var nvidiaRequest =
        new NvidiaEmbeddingRequest(
            texts.toArray(new String[texts.size()]), modelName(), input_type);

    // TODO: XXX No token to pass with the nvidia request for now. This will change on main merge
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY;

    long callStartNano = System.nanoTime();
    return retryHTTPCall(nvidiaClient.embed(accessToken, nvidiaRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var nvidiaResponse = jakartaResponse.readEntity(NvidiaEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (nvidiaResponse.data() == null) {
                throw new IllegalStateException(
                    "ModelProvider %s returned empty data for model %s"
                        .formatted(modelProvider(), modelName()));
              }

              Arrays.sort(nvidiaResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(nvidiaResponse.data())
                      .map(NvidiaEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      nvidiaResponse.usage().prompt_tokens(),
                      nvidiaResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the NVidia Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface NvidiaEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, NvidiaEmbeddingRequest request);
  }

  /**
   * Request structure of the Nidia REST service.
   *
   * <p>..
   */
  public record NvidiaEmbeddingRequest(String[] input, String model, String input_type) {}

  /**
   * Response structure of the Nvidia REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true) // ignore possible extra fields without error
  private record NvidiaEmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }
}
