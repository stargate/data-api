package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class VertexAIEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.VERTEXAI;
  private final VertexAIEmbeddingProviderClient vertexAIEmbeddingProviderClient;

  public VertexAIEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> serviceParameters) {
    super(requestProperties, baseUrl, modelName, dimension, serviceParameters);

    String actualUrl = replaceParameters(baseUrl, serviceParameters);
    vertexAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(VertexAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(NetworkUsageInterceptor.class)
  public interface VertexAIEmbeddingProviderClient {
    @POST
    @Path("/{modelId}:predict")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<jakarta.ws.rs.core.Response> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(providerId, response, errorMessage);
    }

    /**
     * TODO: Add customized error message extraction logic here. <br>
     * Extract the error message from the response body. The example response body is:
     *
     * <pre>
     *
     * </pre>
     *
     * @param response The response body as a String.
     * @return The error message extracted from the response body.
     */
    private static String getErrorMessage(jakarta.ws.rs.core.Response response) {
      // Get the whole response body
      JsonNode rootNode = response.readEntity(JsonNode.class);
      // Log the response body
      logger.info(
          "Error response from embedding provider '{}': {}", providerId, rootNode.toString());
      return rootNode.toString();
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
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());
    EmbeddingRequest request =
        new EmbeddingRequest(texts.stream().map(t -> new EmbeddingRequest.Content(t)).toList());

    Uni<jakarta.ws.rs.core.Response> serviceResponse =
        applyRetry(
            vertexAIEmbeddingProviderClient.embed(
                "Bearer " + embeddingCredentials.apiKey().get(), modelName, request));

    return serviceResponse
        .onItem()
        .transform(
            resp -> {
              EmbeddingResponse embeddingResponse = resp.readEntity(EmbeddingResponse.class);
              if (embeddingResponse.getPredictions() == null) {
                return new Response(
                    batchId,
                    Collections.emptyList(),
                    new VectorizeUsage(ProviderConstants.VERTEXAI, modelName));
              }
              List<float[]> vectors =
                  embeddingResponse.getPredictions().stream()
                      .map(prediction -> prediction.getEmbeddings().getValues())
                      .collect(Collectors.toList());
              int sentBytes = Integer.parseInt(resp.getHeaderString("sent-bytes"));
              int receivedBytes = Integer.parseInt(resp.getHeaderString("received-bytes"));
              VectorizeUsage vectorizeUsage =
                  new VectorizeUsage(
                      sentBytes, receivedBytes, 0, ProviderConstants.VERTEXAI, modelName);
              return new Response(batchId, vectors, vectorizeUsage);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
