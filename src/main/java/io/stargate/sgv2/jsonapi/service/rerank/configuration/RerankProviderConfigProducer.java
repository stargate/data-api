package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.Startup;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.RerankServiceGrpc;
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

public class RerankProviderConfigProducer {
  private static final Logger LOG = LoggerFactory.getLogger(RerankProviderConfigProducer.class);

  @Produces
  @ApplicationScoped
  @Startup
  @Retry(
      maxRetries = 30,
      delay = 6,
      delayUnit = ChronoUnit.SECONDS,
      maxDuration = 180,
      durationUnit = ChronoUnit.SECONDS)
  RerankProvidersConfig produce(
      DefaultRerankProviderConfig defaultRerankProviderConfig,
      OperationsConfig operationsConfig,
      @GrpcClient("embedding") RerankServiceGrpc.RerankServiceBlockingStub rerankService) {
    RerankProvidersConfig defaultConfig =
        new RerankProvidersConfigImpl(defaultRerankProviderConfig.providers());
    // defaultRerankProviderConfig is what we mapped from rerank-providers-config.yaml
    // and will be used if embedding-gateway is not enabled
    if (!operationsConfig.enableEmbeddingGateway()) {
      LOG.info("embedding gateway disabled, use default rerank config");
      return defaultConfig;
    }
    LOG.info("embedding gateway enabled, fetch supported rerank providers from embedding gateway");
    final EmbeddingGateway.GetSupportedRerankProvidersRequest grpcRequest =
        EmbeddingGateway.GetSupportedRerankProvidersRequest.newBuilder().build();
    try {
      EmbeddingGateway.GetSupportedRerankProvidersResponse supportedProvidersResponse =
          rerankService.getSupportedRerankProviders(grpcRequest);
      return grpcResponseToConfig(supportedProvidersResponse);
    } catch (Exception e) {
      throw ErrorCodeV1.SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE.toApiException();
    }
  }

  private RerankProvidersConfig grpcResponseToConfig(
      EmbeddingGateway.GetSupportedRerankProvidersResponse grpcResponse) {
    Map<String, RerankProvidersConfig.RerankProviderConfig> providerMap = new HashMap<>();

    // traverse ProviderConfig in Grpc response
    for (Map.Entry<String, EmbeddingGateway.GetSupportedRerankProvidersResponse.ProviderConfig>
        entry : grpcResponse.getSupportedProvidersMap().entrySet()) {
      // create each rerank provider
      providerMap.put(entry.getKey(), createRerankProviderImpl(entry.getValue()));
    }
    return new RerankProvidersConfigImpl(providerMap);
  }

  private RerankProvidersConfig.RerankProviderConfig createRerankProviderImpl(
      EmbeddingGateway.GetSupportedRerankProvidersResponse.ProviderConfig grpcProviderConfig) {

    Map<
            RerankProvidersConfig.RerankProviderConfig.AuthenticationType,
            RerankProvidersConfig.RerankProviderConfig.AuthenticationConfig>
        supportedAuthenticationsMap =
            grpcProviderConfig.getSupportedAuthenticationsMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        // Convert AuthenticationType string from grpc response to
                        // AuthenticationType
                        eachEntry ->
                            RerankProvidersConfig.RerankProviderConfig.AuthenticationType.valueOf(
                                eachEntry.getKey()),
                        // Construct AuthenticationConfig which includes list of tokenConfig
                        eachEntry -> {
                          EmbeddingGateway.GetSupportedRerankProvidersResponse.ProviderConfig
                                  .AuthenticationConfig
                              grpcProviderAuthConfig = eachEntry.getValue();
                          List<RerankProvidersConfig.RerankProviderConfig.TokenConfig>
                              tokenConfigs =
                                  grpcProviderAuthConfig.getTokensList().stream()
                                      .map(
                                          token ->
                                              new RerankProvidersConfigImpl.RerankProviderConfigImpl
                                                  .AuthenticationConfigImpl.TokenConfigImpl(
                                                  token.getAccepted(), token.getForwarded()))
                                      .collect(Collectors.toList());
                          return new RerankProvidersConfigImpl.RerankProviderConfigImpl
                              .AuthenticationConfigImpl(
                              grpcProviderAuthConfig.getEnabled(), tokenConfigs);
                        }));

    List<RerankProvidersConfig.RerankProviderConfig.ModelConfig> models =
        grpcProviderConfig.getModelsList().stream()
            .map(
                model ->
                    new RerankProvidersConfigImpl.RerankProviderConfigImpl.ModelConfigImpl(
                        model.getName(),
                        model.getIsDefault(),
                        model.getUrl(),
                        new RerankProvidersConfigImpl.RerankProviderConfigImpl.ModelConfigImpl
                            .RequestPropertiesImpl(
                            model.getProperties().getAtMostRetries(),
                            model.getProperties().getInitialBackOffMillis(),
                            model.getProperties().getReadTimeoutMillis(),
                            model.getProperties().getMaxBackOffMillis(),
                            model.getProperties().getJitter(),
                            model.getProperties().getMaxBatchSize())))
            .collect(Collectors.toList());

    return new RerankProvidersConfigImpl.RerankProviderConfigImpl(
        grpcProviderConfig.getIsDefault(),
        grpcProviderConfig.getDisplayName(),
        grpcProviderConfig.getEnabled(),
        supportedAuthenticationsMap,
        models);
  }
}
