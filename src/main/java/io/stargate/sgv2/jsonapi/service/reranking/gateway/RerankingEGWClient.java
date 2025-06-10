package io.stargate.sgv2.jsonapi.service.reranking.gateway;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import java.util.*;
import java.util.stream.Collectors;

/** Grpc client to make reranking Grpc requests to reranking API inside EmbeddingGatewayService */
public class RerankingEGWClient extends RerankingProvider {

  private static final String DEFAULT_TENANT_ID = "default";

  /** Key of authTokens map, for passing Data API token to EGW in grpc request. */
  private static final String DATA_API_TOKEN = "DATA_API_TOKEN";

  /** Key in the authTokens map, for passing Reranking API key to EGW in grpc request. */
  private static final String RERANKING_API_KEY = "RERANKING_API_KEY";

  private final ModelProvider modelProvider;
  private final Optional<String> tenant;
  private final Optional<String> authToken;
  private final String modelName;
  private final RerankingService rerankingGrpcService;
  Map<String, String> authentication;
  private final String commandName;

  public RerankingEGWClient(
      String baseUrl,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
          requestProperties,
      ModelProvider modelProvider,
      Optional<String> tenant,
      Optional<String> authToken,
      String modelName,
      RerankingService rerankingGrpcService,
      Map<String, String> authentication,
      String commandName) {
    super(modelProvider, baseUrl, modelName, requestProperties);

    this.modelProvider = modelProvider;
    this.tenant = tenant;
    this.authToken = authToken;
    this.modelName = modelName;
    this.rerankingGrpcService = rerankingGrpcService;
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
            .setModelName(modelName)
            .setQuery(query)
            .addAllPassages(passages)
            // TODO: Why is the command name passed here ? Can it be removed ?
            .setCommandName(commandName)
            .build();

    var contextBuilder =
        EmbeddingGateway.ProviderRerankingRequest.ProviderContext.newBuilder()
            .setProviderName(modelProvider.apiName())
            .setTenantId(tenant.orElse(DEFAULT_TENANT_ID))
            .putAuthTokens(DATA_API_TOKEN, authToken.orElse(""));
    rerankingCredentials
        .apiKey()
        .ifPresent(v -> contextBuilder.putAuthTokens(RERANKING_API_KEY, v));

    var gatewayRequest =
        EmbeddingGateway.ProviderRerankingRequest.newBuilder()
            .setRerankingRequest(gatewayReranking)
            .setProviderContext(contextBuilder.build())
            .build();

    // TODO: XXX Why is this error handling here not part of the uni pipeline?
    Uni<EmbeddingGateway.RerankingResponse> gatewayRerankingUni;
    try {
      gatewayRerankingUni = rerankingGrpcService.rerank(gatewayRequest);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode().equals(Status.Code.DEADLINE_EXCEEDED)) {
        throw ErrorCodeV1.RERANKING_PROVIDER_TIMEOUT.toApiException(e, e.getMessage());
      }
      throw e;
    }

    return gatewayRerankingUni
        .onItem()
        .transform(
            gatewayResponse -> {
              if (gatewayResponse.hasError()) {
                // TODO : move to V2 error
                throw new JsonApiException(
                    ErrorCodeV1.valueOf(gatewayResponse.getError().getErrorCode()),
                    gatewayResponse.getError().getErrorMessage());
              }

              return new BatchedRerankingResponse(
                  batchId,
                  gatewayResponse.getRanksList().stream()
                      .map(rank -> new Rank(rank.getIndex(), rank.getScore()))
                      .collect(Collectors.toList()),
                  createModelUsage(gatewayResponse.getModelUsage()));
            });
  }
}
