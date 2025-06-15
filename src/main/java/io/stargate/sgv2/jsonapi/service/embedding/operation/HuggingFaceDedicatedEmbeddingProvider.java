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
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class HuggingFaceDedicatedEmbeddingProvider extends EmbeddingProvider {

  public static final String HUGGINGFACE_DEDICATED_ENDPOINT_DEFINED_MODEL =
      "endpoint-defined-model";

  private final HuggingFaceDedicatedEmbeddingProviderClient huggingFaceClient;

  public HuggingFaceDedicatedEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.HUGGINGFACE_DEDICATED,
        providerConfig,
        baseUrl,
        modelConfig,
        dimension,
        vectorizeServiceParameters);

    // replace placeholders: endPointName, regionName, cloudName
    String dedicatedApiUrl = replaceParameters(baseUrl, vectorizeServiceParameters);
    huggingFaceClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(dedicatedApiUrl))
            .readTimeout(providerConfig.properties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(HuggingFaceDedicatedEmbeddingProviderClient.class);
  }

  /**
   * The example response body is:
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
    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());

    var huggingFaceRequest =
        new HuggingFaceDedicatedEmbeddingRequest(texts.toArray(new String[texts.size()]));

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(huggingFaceClient.embed(accessToken, huggingFaceRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var huggingFaceResponse =
                  jakartaResponse.readEntity(HuggingFaceDedicatedEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (huggingFaceResponse.data() == null) {
                throw new IllegalStateException(
                    "ModelProvider %s returned empty data for model %s"
                        .formatted(modelProvider(), modelName()));
              }

              Arrays.sort(huggingFaceResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(huggingFaceResponse.data())
                      .map(HuggingFaceDedicatedEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      huggingFaceResponse.usage().prompt_tokens(),
                      huggingFaceResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the HuggingFace Dedicated Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface HuggingFaceDedicatedEmbeddingProviderClient {
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken,
        HuggingFaceDedicatedEmbeddingRequest request);
  }

  /**
   * Request structure of the HuggingFace Dedicated REST service.
   *
   * <p>huggingfaceDedicated, Test Embeddings Inference, openAI compatible route
   * https://huggingface.github.io/text-embeddings-inference/#/Text%20Embeddings%20Inference/openai_embed
   */
  public record HuggingFaceDedicatedEmbeddingRequest(String[] input) {}

  /**
   * Response structure of the HuggingFace Dedicated REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record HuggingFaceDedicatedEmbeddingResponse(
      String object, Data[] data, String model, Usage usage) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(String object, int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }
}
