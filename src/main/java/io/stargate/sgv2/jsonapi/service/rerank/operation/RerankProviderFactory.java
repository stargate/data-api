package io.stargate.sgv2.jsonapi.service.rerank.operation;

import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class RerankProviderFactory {
  @Inject Instance<RerankProviderConfigStore> rerankProviderConfigStores;
  @Inject RerankProvidersConfig rerankConfig;

  @Inject OperationsConfig config;

  interface ProviderConstructor {
    RerankProvider create(String baseUrl, String modelName);
  }

  private static final Map<String, ProviderConstructor> RERANK_PROVIDER_CONSTRUCTOR_MAP =
      Map.ofEntries(Map.entry("nvidia", NvidiaRerankProvider::new));

  public RerankProvider getConfiguration(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      Map<String, String> authentication,
      String commandName) {
    return addService(tenant, authToken, serviceName, modelName, authentication, commandName);
  }

  private synchronized RerankProvider addService(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      Map<String, String> authentication,
      String commandName) {
    final RerankProvidersConfig.RerankProviderConfig configuration =
        rerankConfig.providers().get(serviceName);
    // TODO(Hazel): add verification for providers and model
    // return the rerank Grpc client to embedding gateway service
    if (config.enableEmbeddingGateway()) {
      // TODO(Yuqi): add egw
    }

    RerankProviderFactory.ProviderConstructor ctor =
        RERANK_PROVIDER_CONSTRUCTOR_MAP.get(serviceName);
    if (ctor == null) {
      throw ErrorCodeV1.RERANK_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", serviceName);
    }
    // TODO(Hazel): need models().get(modelName), but models is a list
    return ctor.create(configuration.models().get(0).url(), modelName);
  }
}
