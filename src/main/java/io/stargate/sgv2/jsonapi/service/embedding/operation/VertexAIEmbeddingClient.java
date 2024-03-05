package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);
  }

  private record EmbeddingRequest(List<Content> instances) {
    public record Content(String content) {}
  }

  private static class EmbeddingResponse {
    public EmbeddingResponse() {}

    private List<Prediction> predictions;

    @JsonIgnore private Object metadata;

    public List<Prediction> getPredictions() {
      return predictions;
    }

    public void setPredictions(List<Prediction> predictions) {
      this.predictions = predictions;
    }

    public Object getMetadata() {
      return metadata;
    }

    public void setMetadata(Object metadata) {
      this.metadata = metadata;
    }

    protected static class Prediction {
      public Prediction() {}

      private Embeddings embeddings;

      public Embeddings getEmbeddings() {
        return embeddings;
      }

      public void setEmbeddings(Embeddings embeddings) {
        this.embeddings = embeddings;
      }

      protected static class Embeddings {
        public Embeddings() {}

        private float[] values;

        @JsonIgnore private Object statistics;

        public float[] getValues() {
          return values;
        }

        public void setValues(float[] values) {
          this.values = values;
        }

        public Object getStatistics() {
          return statistics;
        }

        public void setStatistics(Object statistics) {
          this.statistics = statistics;
        }
      }
    }
  }

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    EmbeddingRequest request =
        new EmbeddingRequest(texts.stream().map(t -> new EmbeddingRequest.Content(t)).toList());
    Uni<EmbeddingResponse> serviceResponse =
        embeddingService.embed(
            "Bearer " + (apiKeyOverride.isPresent() ? apiKeyOverride.get() : apiKey),
            modelName,
            request);
    return serviceResponse
        .onItem()
        .transform(
            response -> {
              return response.getPredictions().stream()
                  .map(prediction -> prediction.getEmbeddings().getValues())
                  .collect(Collectors.toList());
            });
  }
}
