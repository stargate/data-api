package io.stargate.sgv2.jsonapi.service.reranking.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ProviderHttpInterceptor;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
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
 * The Reranking Nvidia Client that sends the request to the Nvidia Reranking Service.
 *
 * <p>Sample http request to self hosted nvidia model - nvidia/llama-3.2-nv-rerankqa-1b-v2:
 *
 * <pre>{@code
 * {
 *   "model": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
 *   "query": {
 *     "text": "which way should i go?"
 *   },
 *   "passages": [
 *     { "text": "left turn" },
 *     { "text": "apple" }
 *   ],
 *   "truncate": "END"
 * }
 * }</pre>
 *
 * <p>Sample response:
 *
 * <pre>{@code
 * {
 *   "rankings": [
 *     { "index": 0, "score": -5.50390625 },
 *     { "index": 1, "score": -11.8828125 }
 *   ],
 *   "usage": {
 *     "prompt_tokens": 26,
 *     "total_tokens": 26
 *   }
 * }
 * }</pre>
 */
public class NvidiaRerankingProvider extends RerankingProvider {

  private final NvidiaRerankingClient nvidiaClient;

  /**
   * Nvidia Reranking Service supports truncation or error behavior when the passage is too long.
   *
   * <p>The Data API uses {@code NONE} as the default, which means the reranking request will error
   * out if there is a query and passage pair that exceeds the allowed token size of 8192.
   *
   * <p>See:
   * https://docs.nvidia.com/nim/nemo-retriever/text-reranking/latest/using-reranking.html#token-limits-truncation
   */
  private static final String TRUNCATE_PASSAGE = "NONE";

  public NvidiaRerankingProvider(
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig) {
    super(ModelProvider.NVIDIA, modelConfig);

    nvidiaClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(modelConfig.url()))
            .readTimeout(10, TimeUnit.MILLISECONDS)
            .build(NvidiaRerankingClient.class);
  }

  @Override
  protected String errorMessageJsonPtr() {
    return "/message";
  }

  @Override
  public Uni<BatchedRerankingResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankingCredentials) {

    // TODO: Move error to v2
    if (rerankingCredentials.apiKey().isEmpty()) {
      throw SchemaException.Code.RERANKING_PROVIDER_AUTHENTICATION_KEY_NOT_PROVIDED.get();
    }
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + rerankingCredentials.apiKey();

    var nvidiaRequest =
        new NvidiaRerankingRequest(
            modelName(),
            new NvidiaRerankingRequest.TextWrapper(query),
            passages.stream().map(NvidiaRerankingRequest.TextWrapper::new).toList(),
            TRUNCATE_PASSAGE);

    final long callStartNano = System.nanoTime();
    return retryHTTPCall(nvidiaClient.rerank(accessToken, nvidiaRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var nvidiaResponse = decodeResponse(jakartaResponse, NvidiaRerankingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // converting from the specific Nvidia response to the generic RerankingBatchResponse
              var ranks =
                  nvidiaResponse.rankings().stream()
                      .map(rank -> new Rank(rank.index(), rank.logit()))
                      .toList();

              var modelUsage =
                  createModelUsage(
                      rerankingCredentials.tenant(),
                      ModelInputType.INPUT_TYPE_UNSPECIFIED,
                      nvidiaResponse.usage().prompt_tokens,
                      nvidiaResponse.usage().total_tokens,
                      jakartaResponse,
                      callDurationNano);
              return new BatchedRerankingResponse(batchId, ranks, modelUsage);
            });
  }

  /**
   * REST client interface for the Nvidia Reranking Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(RerankingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface NvidiaRerankingClient {

    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> rerank(
        @HeaderParam("Authorization") String accessToken, NvidiaRerankingRequest request);
  }

  /**
   * Request structure of the NVIDIA REST service.
   *
   * <p>..
   */
  public record NvidiaRerankingRequest(
      String model, TextWrapper query, List<TextWrapper> passages, String truncate) {

    /**
     * query and passage string needs to be wrapped in with text key for request to the Nvidia
     * Reranking Service. E.G. { "text": "which way should i go?" }
     */
    record TextWrapper(String text) {}
  }

  /**
   * Response structure of hte NVIDIA REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  record NvidiaRerankingResponse(List<NvidiaRanking> rankings, NvidiaUsage usage) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record NvidiaRanking(int index, float logit) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record NvidiaUsage(int prompt_tokens, int total_tokens) {}
  }
}
