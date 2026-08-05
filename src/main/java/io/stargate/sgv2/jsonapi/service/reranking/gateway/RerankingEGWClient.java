package io.stargate.sgv2.jsonapi.service.reranking.gateway;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import java.util.*;

/** Grpc client to make reranking Grpc requests to reranking API inside EmbeddingGatewayService */
public class RerankingEGWClient extends RerankingProvider {

  /** Key of authTokens map, for passing Data API token to EGW in grpc request. */
  private static final String DATA_API_TOKEN = "DATA_API_TOKEN";

  /** Key in the authTokens map, for passing Reranking API key to EGW in grpc request. */
  private static final String RERANKING_API_KEY = "RERANKING_API_KEY";

  private final Tenant tenant;
  private final String authToken;
  private final RerankingService grpcGatewayService;
  Map<String, String> authentication;
  private final String commandName;

  public RerankingEGWClient(
      ModelProvider modelProvider,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig,
      Tenant tenant,
      String authToken,
      RerankingService grpcGatewayService,
      Map<String, String> authentication,
      String commandName) {
    super(modelProvider, modelConfig);

    this.tenant = tenant;
    this.authToken = authToken;
    this.grpcGatewayService = grpcGatewayService;
    this.authentication = authentication;
    this.commandName = commandName;
  }

  @Override
  protected String errorMessageJsonPtr() {
    // not used here, we are just passing through.
    return "";
  }

  @Override
  public Uni<BatchedRerankingResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankingCredentials) {

    var gatewayReranking =
        EmbeddingGateway.ProviderRerankingRequest.RerankingRequest.newBuilder()
            .setModelName(modelName())
            .setQuery(query)
            .addAllPassages(passages)
            // TODO: Why is the command name passed here ? Can it be removed ?
            .setCommandName(commandName)
            .build();

    var contextBuilder =
        EmbeddingGateway.ProviderRerankingRequest.ProviderContext.newBuilder()
            .setProviderName(modelProvider().apiName())
            .setTenantId(tenant.toString())
            .putAuthTokens(DATA_API_TOKEN, authToken);
    if (!rerankingCredentials.apiKey().isEmpty()) {
      contextBuilder.putAuthTokens(RERANKING_API_KEY, rerankingCredentials.apiKey());
    }
    var gatewayRequest =
        EmbeddingGateway.ProviderRerankingRequest.newBuilder()
            .setRerankingRequest(gatewayReranking)
            .setProviderContext(contextBuilder.build())
            .build();

    return Uni.createFrom()
        .deferred(() -> grpcGatewayService.rerank(gatewayRequest))
        .onFailure(StatusRuntimeException.class)
        .transform(this::mapStatusFailure)
        .onItem()
        .transform(
            gatewayResponse -> {
              if (gatewayResponse.hasError()) {
                throw mapGatewayError(gatewayResponse.getError());
              }

              return new BatchedRerankingResponse(
                  batchId,
                  gatewayResponse.getRanksList().stream()
                      .map(rank -> new Rank(rank.getIndex(), rank.getScore()))
                      .toList(),
                  createModelUsage(gatewayResponse.getModelUsage()));
            });
  }

  private Throwable mapStatusFailure(Throwable failure) {
    var statusException = (StatusRuntimeException) failure;
    if (statusException.getStatus().getCode().equals(Status.Code.DEADLINE_EXCEEDED)) {
      return RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.get(
          Map.of(
              "modelProvider",
              modelProvider().apiName(),
              "httpStatus",
              String.valueOf(statusException.getStatus().getCode()),
              "errorMessage",
              statusException.getMessage()));
    }
    return failure;
  }

  private RuntimeException mapGatewayError(EmbeddingGateway.RerankingResponse.ErrorResponse error) {
    String errorCode = error.getErrorCode();

    var schemaCode =
        Arrays.stream(SchemaException.Code.values())
            .filter(code -> code.name().equals(errorCode))
            .findFirst();
    if (schemaCode.isPresent()) {
      return schemaCode.get().withPreformattedMessage(error.getErrorBody());
    }

    var serverCode =
        Arrays.stream(ServerException.Code.values())
            .filter(code -> code.name().equals(errorCode))
            .findFirst();
    if (serverCode.isPresent()) {
      return serverCode.get().withPreformattedMessage(error.getErrorBody());
    }

    var rerankingProviderCode =
        Arrays.stream(RerankingProviderException.Code.values())
            .filter(code -> code.name().equals(errorCode))
            .findFirst();
    if (rerankingProviderCode.isPresent()) {
      return rerankingProviderCode.get().withPreformattedMessage(error.getErrorBody());
    }

    return new RerankingProviderException(errorCode, error.getErrorTitle(), error.getErrorBody());
  }
}
