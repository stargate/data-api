package io.stargate.sgv2.jsonapi.service.provider.reranking.configuration;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.Startup;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankingServiceGrpc;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RerankingProviderConfigProducer {
  private static final Logger LOG = LoggerFactory.getLogger(RerankingProviderConfigProducer.class);

  @Produces
  @ApplicationScoped
  @Startup
  @Retry(
      maxRetries = 30,
      delay = 6,
      delayUnit = ChronoUnit.SECONDS,
      maxDuration = 180,
      durationUnit = ChronoUnit.SECONDS)
  RerankingProvidersConfig produce(
      DefaultRerankingProviderConfig defaultRerankingProviderConfig,
      OperationsConfig operationsConfig,
      @GrpcClient("embedding") RerankingServiceGrpc.RerankingServiceBlockingStub rerankingService) {
    RerankingProvidersConfig defaultConfig =
        new RerankingProvidersConfigImpl(defaultRerankingProviderConfig.providers());
    // defaultRerankingProviderConfig is what we mapped from reranking-providers-config.yaml
    // and will be used if embedding-gateway is not enabled
    if (!operationsConfig.enableEmbeddingGateway()) {
      LOG.info("embedding gateway disabled, use default reranking config");
      return defaultConfig;
    }
    LOG.info(
        "embedding gateway enabled, fetch supported reranking providers from embedding gateway");
    final EmbeddingGateway.GetSupportedRerankingProvidersRequest grpcRequest =
        EmbeddingGateway.GetSupportedRerankingProvidersRequest.newBuilder().build();
    try {
      EmbeddingGateway.GetSupportedRerankingProvidersResponse supportedProvidersResponse =
          rerankingService.getSupportedRerankingProviders(grpcRequest);
      return grpcResponseToConfig(supportedProvidersResponse);
    } catch (Exception e) {
      throw ErrorCodeV1.SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE.toApiException();
    }
  }

  private RerankingProvidersConfig grpcResponseToConfig(
      EmbeddingGateway.GetSupportedRerankingProvidersResponse grpcResponse) {
    Map<String, RerankingProvidersConfig.RerankingProviderConfig> providerMap = new HashMap<>();

    // traverse ProviderConfig in Grpc response
    for (Map.Entry<String, EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig>
        entry : grpcResponse.getSupportedProvidersMap().entrySet()) {
      // create each reranking provider
      providerMap.put(entry.getKey(), createRerankingProviderImpl(entry.getValue()));
    }
    return new RerankingProvidersConfigImpl(providerMap);
  }

  private RerankingProvidersConfig.RerankingProviderConfig createRerankingProviderImpl(
      EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig grpcProviderConfig) {

    Map<
            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType,
            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationConfig>
        supportedAuthenticationsMap =
            grpcProviderConfig.getSupportedAuthenticationsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        // Convert AuthenticationType string from grpc response to
                        // AuthenticationType
                        eachEntry ->
                            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType
                                .valueOf(eachEntry.getKey()),
                        // Construct AuthenticationConfig which includes list of tokenConfig
                        eachEntry -> {
                          EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig
                                  .AuthenticationConfig
                              grpcProviderAuthConfig = eachEntry.getValue();
                          List<RerankingProvidersConfig.RerankingProviderConfig.TokenConfig>
                              tokenConfigs =
                                  grpcProviderAuthConfig.getTokensList().stream()
                                      .map(
                                          token ->
                                              new RerankingProvidersConfigImpl
                                                  .RerankingProviderConfigImpl
                                                  .AuthenticationConfigImpl.TokenConfigImpl(
                                                  token.getAccepted(), token.getForwarded()))
                                      .collect(Collectors.toList());
                          return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl
                              .AuthenticationConfigImpl(
                              grpcProviderAuthConfig.getEnabled(), tokenConfigs);
                        }));

    List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models =
        grpcProviderConfig.getModelsList().stream()
            .map(
                model ->
                    new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
                        model.getName(),
                        model.getIsDefault(),
                        model.getUrl(),
                        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
                            .RequestPropertiesImpl(
                            model.getProperties().getAtMostRetries(),
                            model.getProperties().getInitialBackOffMillis(),
                            model.getProperties().getReadTimeoutMillis(),
                            model.getProperties().getMaxBackOffMillis(),
                            model.getProperties().getJitter(),
                            model.getProperties().getMaxBatchSize())))
            .collect(Collectors.toList());

    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
        grpcProviderConfig.getIsDefault(),
        grpcProviderConfig.getDisplayName(),
        grpcProviderConfig.getEnabled(),
        supportedAuthenticationsMap,
        models);
  }
}
