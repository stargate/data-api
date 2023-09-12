package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.net.URI;
import java.util.List;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class VertexAIEmbeddingClient implements EmbeddingService {
  private String apiKey;
  private String modelName;
  private final VertexAIEmbeddingService embeddingService;

  public VertexAIEmbeddingClient(String baseUrl, String apiKey, String modelName) {
    this.apiKey = apiKey;
    this.modelName = modelName;
    embeddingService =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .build(VertexAIEmbeddingService.class);
  }

  @RegisterRestClient
  public interface VertexAIEmbeddingService {
    @POST
    @Path("/{modelId}:predict")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    List<float[]> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);
  }

  private record EmbeddingRequest(List<Content> instances, Options options) {
    public record Content(String content) {}

    public record Options(boolean waitForModel) {}
  }

  @Override
  public List<float[]> vectorize(List<String> texts) {
    EmbeddingRequest request =
        new EmbeddingRequest(
            texts.stream().map(t -> new EmbeddingRequest.Content(t)).toList(),
            new EmbeddingRequest.Options(true));
    return embeddingService.embed("Bearer " + apiKey, modelName, request);
  }
}
