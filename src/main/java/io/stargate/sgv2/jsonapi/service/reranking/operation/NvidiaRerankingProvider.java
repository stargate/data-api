package io.stargate.sgv2.jsonapi.service.reranking.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.RerankingResponseErrorMessageMapper;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
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

  private static final String providerId = ProviderConstants.NVIDIA;
  private final NvidiaRerankingClient nvidiaRerankingClient;

  // Nvidia Reranking Service supports truncate or error when the passage is too long.
  // Data API use NONE as default, means the reranking request will error out if there is a query
  // and
  // passage pair that exceeds allowed token size 8192
  // https://docs.nvidia.com/nim/nemo-retriever/text-reranking/latest/using-reranking.html#token-limits-truncation
  private static final String TRUNCATE_PASSAGE = "NONE";

  public NvidiaRerankingProvider(
      String baseUrl,
      String modelName,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
          requestProperties) {
    super(baseUrl, modelName, requestProperties);
    nvidiaRerankingClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(NvidiaRerankingClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(RerankingProviderResponseValidation.class)
  public interface NvidiaRerankingClient {

    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<RerankingResponse> rerank(
        @HeaderParam("Authorization") String accessToken, RerankingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return RerankingResponseErrorMessageMapper.mapToAPIException(
          providerId, response, errorMessage);
    }

    private static String getErrorMessage(jakarta.ws.rs.core.Response response) {
      // Get the whole response body
      JsonNode rootNode = response.readEntity(JsonNode.class);
      // Log the response body
      logger.info(
          String.format(
              "Error response from reranking provider '%s': %s", providerId, rootNode.toString()));
      JsonNode messageNode = rootNode.path("message");
      // Return the text of the "message" node, or the whole response body if it is missing
      return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
    }
  }

  /** reranking request to the Nvidia Reranking Service */
  private record RerankingRequest(
      String model, TextWrapper query, List<TextWrapper> passages, String truncate) {
    private record TextWrapper(String text) {}
  }

  /** reranking response from the Nvidia reranking Service */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record RerankingResponse(List<Ranking> rankings, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Ranking(int index, float logit) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<RerankingBatchResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankingCredentials) {

    RerankingRequest request =
        new RerankingRequest(
            modelName,
            new RerankingRequest.TextWrapper(query),
            passages.stream().map(RerankingRequest.TextWrapper::new).toList(),
            TRUNCATE_PASSAGE);

    if (rerankingCredentials.apiKey().isEmpty()) {
      throw ErrorCodeV1.RERANKING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "In order to rerank, please provide the reranking API key.");
    }

    Uni<RerankingResponse> response =
        applyRetry(
            nvidiaRerankingClient.rerank("Bearer " + rerankingCredentials.apiKey().get(), request));

    return response
        .onItem()
        .transform(
            resp -> {
              List<Rank> ranks =
                  resp.rankings().stream()
                      .map(rank -> new Rank(rank.index(), rank.logit()))
                      .toList();
              Usage usage = new Usage(resp.usage().prompt_tokens(), resp.usage().total_tokens());
              return RerankingBatchResponse.of(batchId, ranks, usage);
            });
  }
}
