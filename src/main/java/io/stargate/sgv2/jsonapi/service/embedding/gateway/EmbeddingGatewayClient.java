package io.stargate.sgv2.jsonapi.service.embedding.gateway;

import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Grpc client for embedding gateway service */
public class EmbeddingGatewayClient implements EmbeddingProvider {

  private static final String DEFAULT_TENANT_ID = "default";
  private static final String API_KEY = "API_KEY";

  private EmbeddingProviderConfigStore.RequestProperties requestProperties;

  private String provider;

  private int dimension;

  private Optional<String> tenant;

  private String apiKey;
  private String modelName;
  private String baseUrl;
  private EmbeddingService embeddingService;

  private Map<String, Object> vectorizeServiceParameter;

  /**
   * @param requestProperties
   * @param provider - Embedding provider `openai`, `cohere`, etc
   * @param dimension - Dimension of the embedding to be returned
   * @param tenant - Tenant id {aka database id}
   * @param baseUrl - base url of the embedding client
   * @param modelName - Model name for the embedding provider
   * @param embeddingService - Embedding service client
   * @param vectorizeServiceParameter - Additional parameters for the vectorize service
   */
  public EmbeddingGatewayClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String provider,
      int dimension,
      Optional<String> tenant,
      String baseUrl,
      String modelName,
      EmbeddingService embeddingService,
      Map<String, Object> vectorizeServiceParameter) {
    this.requestProperties = requestProperties;
    this.provider = provider;
    this.dimension = dimension;
    this.tenant = tenant;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    this.embeddingService = embeddingService;
    this.vectorizeServiceParameter = vectorizeServiceParameter;
  }

  /**
   * Vectorize the given list of texts
   *
   * @param texts List of texts to be vectorized
   * @param apiKeyOverride API key sent as header
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return
   */
  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    Map<String, EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue>
        grpcVectorizeServiceParameter = new HashMap<>();
    if (vectorizeServiceParameter != null) {
      vectorizeServiceParameter.forEach(
          (key, value) -> {
            if (value instanceof String)
              grpcVectorizeServiceParameter.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setStrValue((String) value)
                      .build());
            else if (value instanceof Integer)
              grpcVectorizeServiceParameter.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setIntValue((Integer) value)
                      .build());
            else if (value instanceof Float)
              grpcVectorizeServiceParameter.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setFloatValue((Float) value)
                      .build());
            else if (value instanceof Boolean)
              grpcVectorizeServiceParameter.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setBoolValue((Boolean) value)
                      .build());
          });
    }
    EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest embeddingRequest =
        EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.newBuilder()
            .setModelName(modelName)
            .setDimensions(dimension)
            .putAllParameters(grpcVectorizeServiceParameter)
            .setInputType(
                embeddingRequestType == EmbeddingRequestType.INDEX
                    ? EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.InputType.INDEX
                    : EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.InputType.SEARCH)
            .addAllInputs(texts)
            .build();

    final EmbeddingGateway.ProviderEmbedRequest.ProviderContext.Builder builder =
        EmbeddingGateway.ProviderEmbedRequest.ProviderContext.newBuilder()
            .setProviderName(provider)
            .setTenantId(tenant.orElse(DEFAULT_TENANT_ID));
    if (apiKeyOverride.isPresent()) {
      builder.putAuthTokens(API_KEY, apiKeyOverride.orElse(apiKey));
    }

    EmbeddingGateway.ProviderEmbedRequest.ProviderContext providerContext = builder.build();
    EmbeddingGateway.ProviderEmbedRequest providerEmbedRequest =
        EmbeddingGateway.ProviderEmbedRequest.newBuilder()
            .setEmbeddingRequest(embeddingRequest)
            .setProviderContext(providerContext)
            .build();

    final Uni<EmbeddingGateway.EmbeddingResponse> embeddingResponse =
        embeddingService.embed(providerEmbedRequest);
    return embeddingResponse
        .onItem()
        .transform(
            resp -> {
              if (resp.hasError()) {
                throw new JsonApiException(
                    ErrorCode.valueOf(resp.getError().getErrorCode()),
                    resp.getError().getErrorMessage());
              }
              if (resp.getEmbeddingsList() == null) {
                return Collections.emptyList();
              }
              return resp.getEmbeddingsList().stream()
                  .map(
                      data -> {
                        float[] embedding = new float[data.getEmbeddingCount()];
                        for (int i = 0; i < data.getEmbeddingCount(); i++) {
                          embedding[i] = data.getEmbedding(i);
                        }
                        return embedding;
                      })
                  .toList();
            });
  }
}