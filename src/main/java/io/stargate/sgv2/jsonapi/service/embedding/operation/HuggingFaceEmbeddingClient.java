package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class HuggingFaceEmbeddingClient implements EmbeddingService {
  private String apiKey;
  private String modelName;

  private String baseUrl;
  private final HuggingFaceEmbeddingService embeddingService;

  public HuggingFaceEmbeddingClient(String baseUrl, String apiKey, String modelName) {
    this.apiKey = apiKey;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    embeddingService =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .build(HuggingFaceEmbeddingService.class);
  }

  @RegisterRestClient
  public interface HuggingFaceEmbeddingService {
    @POST
    @Path("/{modelId}")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<List<float[]>> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);
  }

  private record EmbeddingRequest(List<String> inputs, Options options) {
    public record Options(boolean waitForModel) {}
  }

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    EmbeddingRequest request = new EmbeddingRequest(texts, new EmbeddingRequest.Options(true));
    return embeddingService.embed(
        "Bearer " + (apiKeyOverride.isPresent() ? apiKeyOverride.get() : apiKey),
        modelName,
        request);
  }
}
