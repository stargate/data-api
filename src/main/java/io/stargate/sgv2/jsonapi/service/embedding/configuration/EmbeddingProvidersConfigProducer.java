package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.runtime.Startup;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingServiceGrpc;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddingProvidersConfigProducer {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingProvidersConfigProducer.class);

  @Produces
  @ApplicationScoped
  @Startup
  @Retry(
      maxRetries = 30,
      delay = 6,
      delayUnit = ChronoUnit.SECONDS,
      maxDuration = 180,
      durationUnit = ChronoUnit.SECONDS)
  EmbeddingProvidersConfig produce(
      DefaultEmbeddingProviderConfig defaultEmbeddingProviderConfig,
      OperationsConfig operationsConfig,
      EmbeddingProvidersConfig.CustomConfig customConfig,
      @GrpcClient("embedding") EmbeddingServiceGrpc.EmbeddingServiceBlockingStub embeddingService) {
    EmbeddingProvidersConfig defaultConfig =
        new EmbeddingProvidersConfigImpl(defaultEmbeddingProviderConfig.providers(), customConfig);
    // defaultEmbeddingProviderConfig is what we mapped from embedding-providers-config.yaml
    // and will be used if embedding-gateway is not enabled
    if (!operationsConfig.enableEmbeddingGateway()) {
      LOG.info("embedding gateway disabled, use default config");
      return defaultConfig;
    }
    LOG.info("embedding gateway enabled, fetch supported providers from embedding gateway");
    final EmbeddingGateway.GetSupportedProvidersRequest getSupportedProvidersRequest =
        EmbeddingGateway.GetSupportedProvidersRequest.newBuilder().build();
    try {
      EmbeddingGateway.GetSupportedProvidersResponse supportedProvidersResponse =
          embeddingService.getSupportedProviders(getSupportedProvidersRequest);
      return grpcResponseToConfig(supportedProvidersResponse, customConfig);
    } catch (Exception e) {
      throw ErrorCode.SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE.toApiException();
    }
  }

  /**
   * @param getSupportedProvidersResponse
   * @return EmbeddingProvidersConfig Convert Grpc response map<string, ProviderConfig>
   *     supportedProviders To EmbeddingProvidersConfig
   */
  private EmbeddingProvidersConfig grpcResponseToConfig(
      EmbeddingGateway.GetSupportedProvidersResponse getSupportedProvidersResponse,
      EmbeddingProvidersConfig.CustomConfig customConfig) {
    Map<String, EmbeddingProvidersConfig.EmbeddingProviderConfig> providerMap = new HashMap<>();

    // traverse ProviderConfig in Grpc GetSupportedProvidersResponse
    for (Map.Entry<String, EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig> entry :
        getSupportedProvidersResponse.getSupportedProvidersMap().entrySet()) {

      final EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig grpcProviderConfig =
          entry.getValue();

      // 1. construct supportedAuthentications map
      Map<
              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType,
              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig>
          supportedAuthenticationsMap =
              grpcProviderConfig.getSupportedAuthenticationsMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          // Convert AuthenticationType string from grpc response to
                          // AuthenticationType
                          eachEntry ->
                              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType
                                  .valueOf(eachEntry.getKey()),
                          // Construct AuthenticationConfig which includes list of tokenConfig in
                          // it.
                          eachEntry -> {
                            EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig
                                    .AuthenticationConfig
                                grpcProviderAuthConfig = eachEntry.getValue();
                            List<EmbeddingProvidersConfig.EmbeddingProviderConfig.TokenConfig>
                                tokenConfigs =
                                    grpcProviderAuthConfig.getTokensList().stream()
                                        .map(
                                            token ->
                                                new EmbeddingProvidersConfigImpl
                                                    .EmbeddingProviderConfigImpl
                                                    .AuthenticationConfigImpl.TokenConfigImpl(
                                                    token.getAccepted(), token.getForwarded()))
                                        .collect(Collectors.toList());
                            return new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl
                                .AuthenticationConfigImpl(
                                grpcProviderAuthConfig.getEnabled(), tokenConfigs);
                          }));

      // 2. construct parameterConfig list for the provider
      List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> providerParameterList =
          grpcProviderConfig.getParametersList().stream()
              .map(
                  EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ParameterConfigImpl::new)
              .collect(Collectors.toList());
      // 3. construct modelConfig list for the provider
      List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig> providerModelList =
          new ArrayList<>();
      final List<EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.ModelConfig>
          grpcProviderConfigModelsList = grpcProviderConfig.getModelsList();
      for (EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.ModelConfig
          grpcModelConfig : grpcProviderConfigModelsList) {
        // construct parameterConfig List for the model
        List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> modelParameterList =
            grpcModelConfig.getParametersList().stream()
                .map(
                    EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ParameterConfigImpl
                        ::new)
                .collect(Collectors.toList());
        providerModelList.add(
            new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
                grpcModelConfig, modelParameterList));
      }

      // 4. construct requestProperties
      final EmbeddingGateway.GetSupportedProvidersResponse.ProviderConfig.RequestProperties
          grpcProviderConfigProperties = grpcProviderConfig.getProperties();
      EmbeddingProvidersConfig.EmbeddingProviderConfig.RequestProperties providerRequestProperties =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
              grpcProviderConfigProperties);

      // construct providerConfig
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
              grpcProviderConfig.getDisplayName(),
              grpcProviderConfig.getEnabled(),
              grpcProviderConfig.getUrl(),
              supportedAuthenticationsMap,
              providerParameterList,
              providerRequestProperties,
              providerModelList);
      providerMap.put(entry.getKey(), providerConfig);
    }
    return new EmbeddingProvidersConfigImpl(providerMap, customConfig);
  }
}
