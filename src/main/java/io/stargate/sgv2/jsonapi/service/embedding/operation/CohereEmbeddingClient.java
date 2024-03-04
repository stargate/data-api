package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen Cohere model.
 */
public class CohereEmbeddingClient implements EmbeddingProvider {
  private String apiKey;
  private String modelName;
  private String baseUrl;
  private final CohereEmbeddingProvider embeddingProvider;

  public CohereEmbeddingClient(String baseUrl, String apiKey, String modelName) {
    this.apiKey = apiKey;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .build(CohereEmbeddingProvider.class);
  }

  @RegisterRestClient
  public interface CohereEmbeddingProvider {
    @POST
    @Path("/embed")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);
  }

  private record EmbeddingRequest(String[] texts, String model, String input_type) {}

  @JsonIgnoreProperties({"id", "texts", "meta", "response_type"})
  private static class EmbeddingResponse {

    protected EmbeddingResponse() {}

    private List<float[]> embeddings;

    public List<float[]> getEmbeddings() {
      return embeddings;
    }

    public void setEmbeddings(List<float[]> embeddings) {
      this.embeddings = embeddings;
    }
  }

  // Input type to be used for vector search should "search_query"
  private static final String SEARCH_QUERY = "search_query";

  @Override
  public Uni<List<float[]>> vectorize(List<String> texts, Optional<String> apiKeyOverride) {
    String[] textArray = new String[texts.size()];
    EmbeddingRequest request =
        new EmbeddingRequest(texts.toArray(textArray), modelName, SEARCH_QUERY);
    Uni<EmbeddingResponse> response =
        embeddingProvider.embed(
            "Bearer " + (apiKeyOverride.isPresent() ? apiKeyOverride.get() : apiKey), request);
    return response
        .onItem()
        .transform(
            resp -> {
              return resp.getEmbeddings();
            });
  }
}
