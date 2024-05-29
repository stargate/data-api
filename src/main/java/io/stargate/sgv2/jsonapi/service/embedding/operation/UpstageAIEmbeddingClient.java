package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class UpstageAIEmbeddingClient implements EmbeddingProvider {
  private static final String UPSTAGE_MODEL_SUFFIX_QUERY = "-query";
  private static final String UPSTAGE_MODEL_SUFFIX_PASSAGE = "-passage";

  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String modelNamePrefix;
  private final UpstageAIEmbeddingProvider embeddingProvider;

  public UpstageAIEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelNamePrefix,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.modelNamePrefix = modelNamePrefix;

    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.timeoutInMillis(), TimeUnit.MILLISECONDS)
            .build(UpstageAIEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface UpstageAIEmbeddingProvider {
    @POST
    // no path specified, as it is already included in the baseUri
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      return HttpResponseErrorMessageMapper.getDefaultException(response);
    }
  }

  // NOTE: "input" is a single String, not array of Strings!
  record EmbeddingRequest(String input, String model) {}

  @JsonIgnoreProperties({"object"})
  record EmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties({"object"})
    record Data(int index, float[] embedding) {}

    record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    // Oddity: Implementation does not support batching, so we only accept "batches"
    // of 1 String, fail for others
    if (texts.size() != 1) {
      // Temporary fail message: with re-batching will give better information
      throw ErrorCode.INVALID_VECTORIZE_VALUE_TYPE.toApiException(
          "UpstageAI only supports vectorization of 1 text at a time, got " + texts.size());
    }
    // Another oddity: model name used as prefix
    final String modelName =
        modelNamePrefix
            + ((embeddingRequestType == EmbeddingRequestType.SEARCH)
                ? UPSTAGE_MODEL_SUFFIX_QUERY
                : UPSTAGE_MODEL_SUFFIX_PASSAGE);

    EmbeddingRequest request = new EmbeddingRequest(texts.get(0), modelName);
    Uni<EmbeddingResponse> response =
        embeddingProvider
            .embed("Bearer " + apiKeyOverride.get(), request)
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
              if (resp.data() == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(resp.data()).map(data -> data.embedding()).toList();
              return Response.of(batchId, vectors);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
