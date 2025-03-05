package io.stargate.sgv2.jsonapi.service.rerank.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.*;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class NvidiaRerankProvider extends RerankProvider {

  // TODO, switch?
  private static final String providerId = ProviderConstants.NVIDIA;
  private final NvidiaRerankClient nvidiaRerankClient;

  public NvidiaRerankProvider(String baseUrl, String modelName) {
    super(baseUrl, modelName);
    nvidiaRerankClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            //             .readTimeout(requestProperties.readTimeoutMillis(),
            // TimeUnit.MILLISECONDS)
            .build(NvidiaRerankProvider.NvidiaRerankClient.class);
  }

  @RegisterRestClient
  //    @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface NvidiaRerankClient {
    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<RerankResponse> rerank(
        @HeaderParam("Authorization") String accessToken,
        NvidiaRerankProvider.RerankRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(providerId, response, errorMessage);
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
  private record RerankRequest(String model, String query, List<String> passages) {}

  /** Rerank response from the Nvidia Rerank Service */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record RerankResponse(List<Ranking> rankings, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Ranking(int index, float logit) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<Response> rerank(int batchId, String query, List<String> passages) {
    NvidiaRerankProvider.RerankRequest request =
        new NvidiaRerankProvider.RerankRequest(modelName, query, passages);
    // Astra-token
    Uni<RerankResponse> response = nvidiaRerankClient.rerank("Bearer ", request);

    return response
        .onItem()
        .transform(
            resp -> {
              // Do we need to sort in this level?
              // resp.ranks.sort(Comparator.comparingInt(RerankResponse.Rank::index));
              List<Rank> ranks =
                  resp.rankings.stream()
                      .map(ranking -> new Rank(ranking.index, ranking.logit))
                      .toList();
              return RerankProvider.Response.of(batchId, ranks);
            });
  }
}
