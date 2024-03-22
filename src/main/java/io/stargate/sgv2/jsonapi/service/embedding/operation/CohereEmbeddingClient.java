package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen Cohere model.
 */
public class CohereEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String apiKey;
  private String modelName;
  private String baseUrl;
  private final CohereEmbeddingProvider embeddingProvider;

  public CohereEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String apiKey,
      String modelName) {
    this.requestProperties = requestProperties;
    this.apiKey = apiKey;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.timeoutInMillis(), TimeUnit.MILLISECONDS)
            .build(CohereEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface CohereEmbeddingProvider {
    @POST
    @Path("/embed")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(Response response) {
      return HttpResponseErrorMessageMapper.getDefaultException(response);
    }
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
  private static final String SEARCH_DOCUMENT = "search_document";

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    String[] textArray = new String[texts.size()];
    String input_type =
        embeddingRequestType == EmbeddingRequestType.INDEX ? SEARCH_DOCUMENT : SEARCH_QUERY;
    EmbeddingRequest request =
        new EmbeddingRequest(texts.toArray(textArray), modelName, input_type);
    Uni<EmbeddingResponse> response =
        embeddingProvider
            .embed(
                "Bearer " + (apiKeyOverride.isPresent() ? apiKeyOverride.get() : apiKey), request)
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
    return response
        .onItem()
        .transform(
            resp -> {
              if (resp.getEmbeddings() == null) {
                return Collections.emptyList();
              }
              return resp.getEmbeddings();
            });
  }
}
