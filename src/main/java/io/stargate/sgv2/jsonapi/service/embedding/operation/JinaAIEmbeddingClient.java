package io.stargate.sgv2.jsonapi.service.embedding.operation;

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
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Implementation of client that talks to jina.ai embedding provider. See <a
 * href="https://api.jina.ai/redoc#tag/embeddings">API reference</a> for details of REST API being
 * called.
 */
public class JinaAIEmbeddingClient implements EmbeddingProvider {
  private EmbeddingProviderConfigStore.RequestProperties requestProperties;
  private String modelName;
  private final JinaAIEmbeddingProvider embeddingProvider;

  public JinaAIEmbeddingClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.modelName = modelName;
    embeddingProvider =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.timeoutInMillis(), TimeUnit.MILLISECONDS)
            .build(JinaAIEmbeddingProvider.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface JinaAIEmbeddingProvider {
    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(Response response) {
      return HttpResponseErrorMessageMapper.getDefaultException(response);
    }
  }

  // By default, Jina Text Encoding Format is float
  private record EmbeddingRequest(List<String> input, String model) {}

  private record EmbeddingResponse(String object, Data[] data, String model, Usage usage) {
    private record Data(String object, int index, float[] embedding) {}

    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    EmbeddingRequest request = new EmbeddingRequest(texts, modelName);
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
            // Jina has intermittent embedding failure, set a longer retry delay to mitigate the
            // issue
            .withBackOff(Duration.ofMillis(200), Duration.ofMillis(800))
            .atMost(requestProperties.maxRetries());
    return response
        .onItem()
        .transform(
            resp -> {
              if (resp.data() == null) {
                return Collections.emptyList();
              }
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              return Arrays.stream(resp.data()).map(EmbeddingResponse.Data::embedding).toList();
            });
  }
}
