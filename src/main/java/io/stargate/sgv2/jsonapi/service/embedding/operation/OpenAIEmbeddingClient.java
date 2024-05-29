package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import io.stargate.sgv2.jsonapi.service.embedding.util.EmbeddingUtil;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
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

public class OpenAIEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String modelName;
  private int dimension;
  private final OpenAIEmbeddingProvider embeddingProvider;
  private Map<String, Object> vectorizeServiceParameters;

  public OpenAIEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.modelName = modelName;
    // One special case: legacy "ada-002" model does not accept "dimension" parameter
    this.dimension = EmbeddingUtil.acceptsOpenAIDimensions(modelName) ? dimension : 0;
    this.vectorizeServiceParameters = vectorizeServiceParameters;
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.timeoutInMillis(), TimeUnit.MILLISECONDS)
            .build(OpenAIEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface OpenAIEmbeddingProvider {
    @POST
    @Path("/embeddings")
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      return HttpResponseErrorMessageMapper.getDefaultException(response);
    }
  }

  private record EmbeddingRequest(
      String[] input,
      String model,
      @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  private record EmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    private record Data(String object, int index, float[] embedding) {}

    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    String[] textArray = new String[texts.size()];
    EmbeddingRequest request = new EmbeddingRequest(texts.toArray(textArray), modelName, dimension);
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
  public int batchSize() {
    return requestProperties.batchSize();
  }
}
