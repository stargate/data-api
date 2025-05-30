package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.EmbeddingProviderErrorMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
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
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
      int dimension,
      Map<String, Object> serviceParameters,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    super(requestProperties, baseUrl, model, dimension, serviceParameters, providerConfig);

    String actualUrl = replaceParameters(baseUrl, serviceParameters);
    vertexAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(VertexAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface VertexAIEmbeddingProviderClient {
    @POST
    @Path("/{modelId}:predict")
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken,
        @PathParam("modelId") String modelId,
        EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return EmbeddingProviderErrorMapper.mapToAPIException(providerId, response, errorMessage);
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
      logger.error(
          "Error response from embedding provider '{}': {}", providerId, rootNode.toString());
      return rootNode.toString();
    }
  }

  private record EmbeddingRequest(List<Content> instances) {
    public record Content(String content) {}
  }

  @JsonIgnoreProperties(ignoreUnknown = true) // ignore possible extra fields without error
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class Prediction {
      public Prediction() {}

      private Embeddings embeddings;

      public Embeddings getEmbeddings() {
        return embeddings;
      }

      public void setEmbeddings(Embeddings embeddings) {
        this.embeddings = embeddings;
      }

      @JsonIgnoreProperties(ignoreUnknown = true)
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
    // Check if using an EOF model
    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(providerId, embeddingCredentials.apiKey());
    EmbeddingRequest request =
        new EmbeddingRequest(texts.stream().map(t -> new EmbeddingRequest.Content(t)).toList());

    Uni<EmbeddingResponse> serviceResponse =
        applyRetry(
            vertexAIEmbeddingProviderClient.embed(
                HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get(),
                model.name(),
                request));

    return serviceResponse
        .onItem()
        .transform(
            response -> {
              if (response.getPredictions() == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              List<float[]> vectors =
                  response.getPredictions().stream()
                      .map(prediction -> prediction.getEmbeddings().getValues())
                      .collect(Collectors.toList());
              return Response.of(batchId, vectors);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
