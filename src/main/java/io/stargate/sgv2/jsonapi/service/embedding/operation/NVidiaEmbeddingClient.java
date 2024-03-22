package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.EMBEDDING_PROVIDER_INVALID_RESPONSE;

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
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen nvidia model.
 */
public class NVidiaEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String apiKey;
  private String modelName;
  private String baseUrl;
  private final NVidiaEmbeddingProvider embeddingProvider;

  public NVidiaEmbeddingClient(
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
            .build(NVidiaEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface NVidiaEmbeddingProvider {
    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(Response response) {
      return HttpResponseErrorMessageMapper.getDefaultException(response);
    }
  }

  private record EmbeddingRequest(String[] input, String model, String input_type) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record EmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  private static final String PASSAGE = "passage";
  private static final String QUERY = "query";

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    String[] textArray = new String[texts.size()];
    String input_type = embeddingRequestType == EmbeddingRequestType.INDEX ? PASSAGE : QUERY;

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

    return response
        .onItem()
        .transform(
            resp -> {
              validateEmbeddingResponse(resp);
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              return Arrays.stream(resp.data()).map(data -> data.embedding()).toList();
            });
  }

  private void validateEmbeddingResponse(EmbeddingResponse response) {
    // if there is no data return
    if (response.data() == null || response.data().length == 0) {
      throw EMBEDDING_PROVIDER_INVALID_RESPONSE.toApiException(
          "No data return from the embedding provider");
    }
  }
}
