package io.stargate.sgv2.jsonapi.service.embedding.gateway;

import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.ArrayUtils;

public class EmbeddingGatewayClient implements EmbeddingProvider {

  private EmbeddingProviderConfigStore.RequestProperties requestProperties;

  private String provider;

  private int dimension;

  private Optional<String> tenant;

  private String apiKey;
  private String modelName;
  private String baseUrl;
  private EmbeddingService embeddingService;

  public EmbeddingGatewayClient(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String provider,
      int dimension,
      Optional<String> tenant,
      String baseUrl,
      String apiKey,
      String modelName,
      EmbeddingService embeddingService) {
    this.requestProperties = requestProperties;
    this.provider = provider;
    this.dimension = dimension;
    this.tenant = tenant;
    this.apiKey = apiKey;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    this.embeddingService = embeddingService;
  }

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest embeddingRequest =
        EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.newBuilder()
            .setModelName(modelName)
            .setDimensions(dimension)
            .setInputType(
                embeddingRequestType == EmbeddingRequestType.INDEX
                    ? EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.InputType.INDEX
                    : EmbeddingGateway.ProviderEmbedRequest.EmbeddingRequest.InputType.SEARCH)
            .addAllInputs(texts)
            .build();

    final EmbeddingGateway.ProviderEmbedRequest.ProviderContext.Builder builder =
        EmbeddingGateway.ProviderEmbedRequest.ProviderContext.newBuilder()
            .setProviderName(provider)
            .setTenantId(tenant.orElse("default"));
    if (apiKeyOverride.isPresent()) {
      builder.putAuthTokens("API_KEY", apiKeyOverride.orElse(apiKey));
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
                        Float[] embedding = new Float[data.getEmbeddingList().size()];
                        data.getEmbeddingList().toArray(embedding);
                        return ArrayUtils.toPrimitive(embedding);
                      })
                  .toList();
            });
  }
}
