package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EmbeddingProvidersConfigProducer {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingProvidersConfigProducer.class);

  @Inject OperationsConfig operationsConfig;

  @GrpcClient EmbeddingService embeddingService;

  @Produces
  EmbeddingProvidersConfig produce(FileEmbeddingProviderConfig fileEmbeddingProvidersConfigStore) {
    EmbeddingProvidersConfig defaultConfig =
        new EmbeddingProvidersConfigImpl(
            fileEmbeddingProvidersConfigStore.providers(),
            new EmbeddingProvidersConfigImpl.CustomConfigImpl());
    // FileEmbeddingProviderConfig is what we mapped from embedding-providers-config.yaml
    // and will be used if embedding-gateway is not enabled
    if (!operationsConfig.enableEmbeddingGateway()) {
      LOG.info("embedding gateway disabled, use default config");
      return defaultConfig;
    }
    LOG.info("embedding gateway enabled, fetch supported providers from embedding gateway");
    final EmbeddingGateway.GetSupportedProvidersRequest getSupportedProvidersRequest =
        EmbeddingGateway.GetSupportedProvidersRequest.newBuilder().build();
    final Uni<EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProvidersResponseUni =
        embeddingService.getSupportedProviders(getSupportedProvidersRequest);
    EmbeddingGateway.GetSupportedProvidersResponse getSupportedProvidersResponse = null;
    try {
      getSupportedProvidersResponse =
          getSupportedProvidersResponseUni
              .onItem()
              .transform(
                  Unchecked.function(
                      resp -> {
                        if (resp.hasError()) {
                          throw new JsonApiException(
                              ErrorCode.valueOf(resp.getError().getErrorCode()),
                              resp.getError().getErrorMessage());
                        }
                        return resp;
                      }))
              .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
              .subscribeAsCompletionStage()
              .get();
    } catch (Exception e) {
      LOG.error(
          "embedding gateway enabled, but fail to fetch supported providers, use default config, exception: "
              + e);
      return defaultConfig;
    }
    return grpcResponseToConfig(getSupportedProvidersResponse);
  }

  /**
   * @param getSupportedProvidersResponse
   * @return EmbeddingProvidersConfig Convert Grpc response map<string, ProviderConfig>
   *     supportedProviders To EmbeddingProvidersConfig
   */
  private EmbeddingProvidersConfig grpcResponseToConfig(
      EmbeddingGateway.GetSupportedProvidersResponse getSupportedProvidersResponse) {
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
                          eachEntry ->
                              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType
                                  .valueOf(eachEntry.getKey()),
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
              grpcProviderConfig.getEnabled(),
              grpcProviderConfig.getUrl(),
              supportedAuthenticationsMap,
              providerParameterList,
              providerRequestProperties,
              providerModelList);
      providerMap.put(entry.getKey(), providerConfig);
    }
    return new EmbeddingProvidersConfigImpl(providerMap, null);
  }
}
