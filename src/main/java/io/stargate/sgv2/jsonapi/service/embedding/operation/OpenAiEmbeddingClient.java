package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class OpenAiEmbeddingClient implements EmbeddingService {
  private String apiKey;
  private String modelName;
  private String baseUrl;
  private final OpenAiEmbeddingService embeddingService;

  public OpenAiEmbeddingClient(String baseUrl, String apiKey, String modelName) {
    this.apiKey = apiKey;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    embeddingService =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .build(OpenAiEmbeddingService.class);
  }

  @RegisterRestClient
  public interface OpenAiEmbeddingService {
    @POST
    @Path("/embeddings")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);
  }

  private record EmbeddingRequest(String[] input, String model) {}

  private record EmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    private record Data(String object, int index, float[] embedding) {}

    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<List<float[]>> vectorize(List<String> texts) {
    String[] textArray = new String[texts.size()];
    EmbeddingRequest request = new EmbeddingRequest(texts.toArray(textArray), modelName);
    Uni<EmbeddingResponse> response = embeddingService.embed("Bearer " + apiKey, request);
    return response
        .onItem()
        .transform(
            resp -> {
              return Arrays.stream(response.data()).map(data -> data.embedding()).toList();
            });
  }
}
