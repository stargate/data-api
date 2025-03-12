package io.stargate.sgv2.jsonapi.service.rerank.gateway;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankService;
import io.stargate.sgv2.jsonapi.api.request.RerankCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import io.stargate.sgv2.jsonapi.service.rerank.operation.RerankProvider;
import java.util.*;
import java.util.stream.Collectors;

/** Grpc client to make rerank Grpc requests to rerank API inside EmbeddingGatewayService */
public class RerankEGWClient extends RerankProvider {

  private static final String DEFAULT_TENANT_ID = "default";

  /** Map to the value of `Token` in the header */
  private static final String DATA_API_TOKEN = "DATA_API_TOKEN";

  /**
   * key name of the rerank service provider. Note, the request self-hosted Nvidia reranker service
   * does not need to specify an auth token in Data API request header. Instead, we use the Astra
   * token to authenticate the request.
   */
  private static final String RERANK_API_KEY = "RERANK_API_KEY";

  private String provider;

  private Optional<String> tenant;
  private Optional<String> authToken;
  private String modelName;
  private RerankService rerankGrpcService;
  Map<String, String> authentication;
  private String commandName;

  public RerankEGWClient(
      String baseUrl,
      RerankProvidersConfig.RerankProviderConfig.ModelConfig.RequestProperties requestProperties,
      String provider,
      Optional<String> tenant,
      Optional<String> authToken,
      String modelName,
      RerankService rerankGrpcService,
      Map<String, String> authentication,
      String commandName) {
    super(baseUrl, modelName, requestProperties);
    this.provider = provider;
    this.tenant = tenant;
    this.authToken = authToken;
    this.modelName = modelName;
    this.rerankGrpcService = rerankGrpcService;
    this.authentication = authentication;
    this.commandName = commandName;
  }

  @Override
  public Uni<RerankBatchResponse> rerank(
      int batchId, String query, List<String> passages, RerankCredentials rerankCredentials) {

    // Build the rerank provider request in grpc request
    final EmbeddingGateway.ProviderRerankRequest.RerankRequest rerankRequest =
        EmbeddingGateway.ProviderRerankRequest.RerankRequest.newBuilder()
            .setModelName(modelName)
            .setQuery(query)
            .addAllPassages(passages)
            .setCommandName(commandName)
            .build();

    // Build the rerank provider context in grpc request
    final EmbeddingGateway.ProviderRerankRequest.ProviderContext providerContext =
        EmbeddingGateway.ProviderRerankRequest.ProviderContext.newBuilder()
            .setProviderName(provider)
            .setTenantId(tenant.orElse(DEFAULT_TENANT_ID))
            .putAuthTokens(DATA_API_TOKEN, authToken.orElse(""))
            .putAuthTokens(RERANK_API_KEY, authToken.orElse(""))
            .build();

    // Built the Grpc request
    final EmbeddingGateway.ProviderRerankRequest grpcRerankRequest =
        EmbeddingGateway.ProviderRerankRequest.newBuilder()
            .setRerankRequest(rerankRequest)
            .setProviderContext(providerContext)
            .build();

    Uni<EmbeddingGateway.RerankResponse> grpcRerankResponse;
    try {
      grpcRerankResponse = rerankGrpcService.rerank(grpcRerankRequest);
    } catch (StatusRuntimeException e) {
      // TODO, ???
      if (e.getStatus().getCode().equals(Status.Code.DEADLINE_EXCEEDED)) {
        throw ErrorCodeV1.RERANK_PROVIDER_TIMEOUT.toApiException(e, e.getMessage());
      }
      throw e;
    }

    return grpcRerankResponse
        .onItem()
        .transform(
            resp -> {
              if (resp.hasError()) {
                throw new JsonApiException(
                    ErrorCodeV1.valueOf(resp.getError().getErrorCode()),
                    resp.getError().getErrorMessage());
              }
              return RerankBatchResponse.of(
                  batchId,
                  resp.getRanksList().stream()
                      .map(rank -> new Rank(rank.getIndex(), rank.getScore()))
                      .collect(Collectors.toList()),
                  new Usage(resp.getUsage().getPromptTokens(), resp.getUsage().getTotalTokens()));
            });
  }
}
