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

  public HuggingFaceEmbeddingClient(String apiKey, String modelName) {
    this("https://api-inference.huggingface.co", apiKey, modelName);
  }

  @RegisterRestClient
  public interface HuggingFaceEmbeddingService {
    @POST
    @Path("/{modelId}")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    List<float[]> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);
  }

  private record EmbeddingRequest(List<String> inputs, Options options) {
    public record Options(boolean waitForModel) {}
  }

  @Override
  public List<float[]> vectorize(List<String> texts) {
    EmbeddingRequest request = new EmbeddingRequest(texts, new EmbeddingRequest.Options(true));
    return embeddingService.embed("Bearer " + apiKey, modelName, request);
  }
}
