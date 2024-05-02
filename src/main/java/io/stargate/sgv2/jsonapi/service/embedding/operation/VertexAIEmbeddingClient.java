package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class VertexAIEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String modelName;
  private final VertexAIEmbeddingProvider embeddingProvider;

  private static final String PROJECT_ID = "PROJECT_ID";

  private Map<String, Object> vectorizeServiceParameters;

  public VertexAIEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.modelName = modelName;
    this.vectorizeServiceParameters = vectorizeServiceParameters;
    baseUrl = baseUrl.replace(PROJECT_ID, vectorizeServiceParameters.get(PROJECT_ID).toString());
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.timeoutInMillis(), TimeUnit.MILLISECONDS)
            .build(VertexAIEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface VertexAIEmbeddingProvider {
    @POST
    @Path("/{modelId}:predict")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(Response response) {
      return HttpResponseErrorMessageMapper.getDefaultException(response);
    }
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
        embeddingProvider
            .embed("Bearer " + apiKeyOverride.get(), modelName, request)
            .onFailure(
                throwable -> {
                  return (throwable.getCause() != null
                      && throwable.getCause() instanceof JsonApiException jae
                      && jae.getErrorCode() == ErrorCode.EMBEDDING_PROVIDER_TIMEOUT);
                })
            .retry()
            .withBackOff(Duration.ofMillis(requestProperties.retryDelayInMillis()))
            .atMost(requestProperties.maxRetries());
    ;
    return serviceResponse
        .onItem()
        .transform(
            response -> {
              if (response.getPredictions() == null) {
                return Collections.emptyList();
              }
              return response.getPredictions().stream()
                  .map(prediction -> prediction.getEmbeddings().getValues())
                  .collect(Collectors.toList());
            });
  }
}
