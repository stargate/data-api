package io.stargate.sgv2.jsonapi.service.embedding.gateway;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import java.util.*;

/** Grpc client for embedding gateway service */
public class EmbeddingGatewayClient extends EmbeddingProvider {

  private static final String DEFAULT_TENANT_ID = "default";

  /** Map to the value of `x-embedding-api-key` in the header */
  private static final String EMBEDDING_API_KEY = "EMBEDDING_API_KEY";

  /** Map to the value of `x-embedding-access-id` in the header */
  private static final String EMBEDDING_ACCESS_ID = "EMBEDDING_ACCESS_ID";

  /** Map to the value of `x-embedding-secret-id` in the header */
  private static final String EMBEDDING_SECRET_ID = "EMBEDDING_SECRET_ID";

  /** Map to the value of `Token` in the header */
  private static final String DATA_API_TOKEN = "DATA_API_TOKEN";

  private ServiceConfigStore.ServiceRequestProperties requestProperties;

  private Tenant tenant;
  private String authToken;
  private EmbeddingService grpcGatewayClient;
  Map<String, String> authentication;
  private String commandName;

  /** */
  public EmbeddingGatewayClient(
      ModelProvider modelProvider,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Tenant tenant,
      String authToken,
      EmbeddingService grpcGatewayClient,
      Map<String, String> authentication,
      String commandName) {
    super(
        modelProvider,
        providerConfig,
        modelConfig,
        serviceConfig,
        dimension,
        vectorizeServiceParameters);

    this.tenant = tenant;
    this.authToken = authToken;
    this.grpcGatewayClient = grpcGatewayClient;
    this.authentication = authentication;
    this.commandName = commandName;
  }

  @Override
  protected String errorMessageJsonPtr() {
    // not used , this is passing through the grpc error
    return "";
  }

  /**
   * Vectorize the given list of texts
   *
   * @param texts List of texts to be vectorized
   * @param embeddingCredentials Credentials required for the provider
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return
   */
  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    var gatewayRequestParams =
        new HashMap<
            String, EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue>();

    if (vectorizeServiceParameters != null) {
      vectorizeServiceParameters.forEach(
          (key, value) -> {
            if (value instanceof String)
              gatewayRequestParams.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setStrValue((String) value)
                      .build());
            else if (value instanceof Integer)
              gatewayRequestParams.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setIntValue((Integer) value)
                      .build());
            else if (value instanceof Float)
              gatewayRequestParams.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setFloatValue((Float) value)
                      .build());
            else if (value instanceof Boolean)
              gatewayRequestParams.put(
                  key,
                  EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.ParameterValue.newBuilder()
                      .setBoolValue((Boolean) value)
                      .build());
          });
    }

    var gatewayEmbedding =
        EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.newBuilder()
            .setModelName(modelName())
            .setDimensions(dimension)
            .setCommandName(commandName)
            .putAllParameters(gatewayRequestParams)
            .setInputType(
                embeddingRequestType == EmbeddingRequestType.INDEX
                    ? EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.InputType.INDEX
                    : EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.InputType.SEARCH)
            .addAllInputs(texts)
            .build();

    var contextBuilder =
        EmbeddingGateway.ProviderEmbedRequest.ProviderContext.newBuilder()
            .setProviderName(modelProvider().apiName())
            .setTenantId(tenant.toString())
            .putAuthTokens(DATA_API_TOKEN, authToken);

    embeddingCredentials
        .apiKey()
        .ifPresent(v -> contextBuilder.putAuthTokens(EMBEDDING_API_KEY, v));
    embeddingCredentials
        .accessId()
        .ifPresent(v -> contextBuilder.putAuthTokens(EMBEDDING_ACCESS_ID, v));
    embeddingCredentials
        .secretId()
        .ifPresent(v -> contextBuilder.putAuthTokens(EMBEDDING_SECRET_ID, v));

    // Add the `authentication` (sync service key) in the createCollection command
    if (authentication != null) {
      contextBuilder.putAllAuthTokens(authentication);
    }

    var gatewayRequest =
        EmbeddingGateway.ProviderEmbedRequest.newBuilder()
            .setEmbeddingRequest(gatewayEmbedding)
            .setProviderContext(contextBuilder.build())
            .build();

    // aaron 17 June 2025 - unsure why this error handled was not in the uni pipleine below
    // kept it as is when refactoring
    Uni<EmbeddingGateway.EmbeddingResponse> embeddingResponse;
    try {
      embeddingResponse = grpcGatewayClient.embed(gatewayRequest);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode().equals(Status.Code.DEADLINE_EXCEEDED)) {
        throw EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT.get(
            Map.of(
                "provider",
                modelProvider().apiName(),
                "httpStatus",
                String.valueOf(e.getStatus().getCode()),
                "errorMessage",
                e.getMessage()));
      }
      throw e;
    }

    return embeddingResponse
        .onItem()
        .transform(
            gatewayResponse -> {
              if (gatewayResponse.hasError()) {
                throw new EmbeddingProviderException(
                    gatewayResponse.getError().getErrorCode(),
                    gatewayResponse.getError().getErrorTitle(),
                    gatewayResponse.getError().getErrorBody());
              }
              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // but grpc will make sure resp.getEmbeddingsList() is never null

              final List<float[]> vectors =
                  gatewayResponse.getEmbeddingsList().stream()
                      .map(
                          data -> {
                            float[] embedding = new float[data.getEmbeddingCount()];
                            for (int i = 0; i < data.getEmbeddingCount(); i++) {
                              embedding[i] = data.getEmbedding(i);
                            }
                            return embedding;
                          })
                      .toList();
              return new BatchedEmbeddingResponse(
                  batchId, vectors, createModelUsage(gatewayResponse.getModelUsage()));
            });
  }

  /**
   * Return MAX_VALUE because the batching is done inside EGW
   *
   * @return
   */
  @Override
  public int maxBatchSize() {
    return Integer.MAX_VALUE;
  }
}
