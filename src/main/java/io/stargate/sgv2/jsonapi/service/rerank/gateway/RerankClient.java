package io.stargate.sgv2.jsonapi.service.rerank.gateway;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankService;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.rerank.operation.RerankProvider;
import java.util.*;

/** Grpc client to make rerank Grpc requests to rerank API inside EmbeddingGatewayService */
public class RerankClient extends RerankProvider {

  private static final String DEFAULT_TENANT_ID = "default";

  /** Map to the value of `Token` in the header */
  private static final String DATA_API_TOKEN = "DATA_API_TOKEN";

  private EmbeddingProviderConfigStore.RequestProperties requestProperties;

  private String provider;

  private Optional<String> tenant;
  private Optional<String> authToken;
  private String modelName;
  private String baseUrl;
  private RerankService rerankService;
  Map<String, String> authentication;
  private String commandName;

  public RerankClient(
      String provider,
      Optional<String> tenant,
      Optional<String> authToken,
      String baseUrl,
      String modelName,
      RerankService rerankService,
      Map<String, String> authentication,
      String commandName) {
    this.provider = provider;
    this.tenant = tenant;
    this.authToken = authToken;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    this.rerankService = rerankService;
    this.authentication = authentication;
    this.commandName = commandName;
  }

  @Override
  public Uni<Response> rerank(int batchId, String query, List<String> passages) {

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
            .build();

    // Built the Grpc request
    final EmbeddingGateway.ProviderRerankRequest grpcRerankRequest =
        EmbeddingGateway.ProviderRerankRequest.newBuilder()
            .setRerankRequest(rerankRequest)
            .setProviderContext(providerContext)
            .build();

    Uni<EmbeddingGateway.RerankResponse> grpcRerankResponse;
    try {
      grpcRerankResponse = rerankService.rerank(grpcRerankRequest);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode().equals(Status.Code.DEADLINE_EXCEEDED)) {
        // TODO, not embedding provider
        throw ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException(e, e.getMessage());
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
              return Response.of(batchId, null);
              //                  resp.getRankingsList().stream()
              //                      .map(rank -> new Rank(rank.getIndex(), rank.getLogit()))
              //                      .collect(Collectors.toList()));
            });
  }
}
