package io.stargate.sgv2.jsonapi.service.rerank.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.RerankResponseErrorMessageMapper;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * The Rerank Nvidia Client that sends the request to the Nvidia Rerank Service.
 *
 * <p>Sample http request to self-host nvidia nvidia/llama-3.2-nv-rerankqa-1b-v2: { "model":
 * "nvidia/llama-3.2-nv-rerankqa-1b-v2", "query": { "text": "which way should i go?" }, "passages":
 * [ { "text": "left turn" }, { "text": "apple" } ], "truncate": "END" }
 *
 * <p>Sample response { "rankings": [ { "index": 0, "score": -5.50390625 }, { "index": 1, "score":
 * -11.8828125 } ], "usage": { "prompt_tokens": 26, "total_tokens": 26 } }
 */
public class NvidiaRerankProvider extends RerankProvider {

  private static final String providerId = ProviderConstants.NVIDIA;
  private final NvidiaRerankClient nvidiaRerankClient;

  // Nvidia Rerank Service supports truncate or error when the passage is too long.
  // Data API use NONE as default, means the rerank request will error out if there is a query and
  // passage pair that exceeds allowed token size 8192
  // https://docs.nvidia.com/nim/nemo-retriever/text-reranking/latest/using-reranking.html#token-limits-truncation
  private static final String TRUNCATE_PASSAGE = "NONE";

  public NvidiaRerankProvider(
      String baseUrl,
      String modelName,
      RerankProvidersConfig.RerankProviderConfig.ModelConfig.RequestProperties requestProperties) {
    super(baseUrl, modelName, requestProperties);
    nvidiaRerankClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(NvidiaRerankProvider.NvidiaRerankClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(RerankProviderResponseValidation.class)
  public interface NvidiaRerankClient {

    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<RerankResponse> rerank(
        @HeaderParam("Authorization") String accessToken,
        NvidiaRerankProvider.RerankRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return RerankResponseErrorMessageMapper.mapToAPIException(providerId, response, errorMessage);
    }

    private static String getErrorMessage(jakarta.ws.rs.core.Response response) {
      // Get the whole response body
      JsonNode rootNode = response.readEntity(JsonNode.class);
      // Log the response body
      logger.info(
          String.format(
              "Error response from rerank provider '%s': %s", providerId, rootNode.toString()));
      JsonNode messageNode = rootNode.path("message");
      // Return the text of the "message" node, or the whole response body if it is missing
      return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
    }
  }

  /** Rerank request to the Nvidia Rerank Service */
  private record RerankRequest(
      String model, TextWrapper query, List<TextWrapper> passages, String truncate) {
    private record TextWrapper(String text) {}
  }

  /** Rerank response from the Nvidia Rerank Service */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record RerankResponse(List<Ranking> rankings, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Ranking(int index, float logit) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<RerankBatchResponse> rerank(
      int batchId, String query, List<String> passages, RerankCredentials rerankCredentials) {

    NvidiaRerankProvider.RerankRequest request =
        new NvidiaRerankProvider.RerankRequest(
            modelName,
            new RerankRequest.TextWrapper(query),
            passages.stream().map(RerankRequest.TextWrapper::new).toList(),
            TRUNCATE_PASSAGE);

    // Note, Nvidia self-host reranker service use Astra token to authenticate the request.
    // So we use token in rerankCredentials, not apiKey in rerankCredentials.
    Uni<RerankResponse> response =
        applyRetry(
            nvidiaRerankClient.rerank("Bearer " + resolveRerankKey(rerankCredentials), request));

    return response
        .onItem()
        .transform(
            resp -> {
              List<Rank> ranks =
                  resp.rankings().stream()
                      .map(rank -> new Rank(rank.index(), rank.logit()))
                      .toList();
              Usage usage = new Usage(resp.usage().prompt_tokens(), resp.usage().total_tokens());
              return RerankProvider.RerankBatchResponse.of(batchId, ranks, usage);
            });
  }

  /**
   * For Astra self-hosted Nvidia rerank in the GPU plane, it requires the AstraCS token to access.
   * So Data API in Astra will resolve the AstraCS token from the request header. For Data API in
   * non-astra environment, since the token is also used for backend authentication, so the user
   * needs to pass the rerank API key in the request header 'x-rerank-api-key'.
   */
  private String resolveRerankKey(RerankCredentials rerankCredentials) {
    if (rerankCredentials.token().startsWith("AstraCS")) {
      return rerankCredentials.token();
    }
    if (rerankCredentials.apiKey().isEmpty()) {
      throw ErrorCodeV1.RERANK_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "In order to rerank, please add the rerank API key in the request header 'x-rerank-api-key' for non-astra environment.");
    }
    return rerankCredentials.apiKey().get();
  }
}
