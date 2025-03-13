package io.stargate.sgv2.jsonapi.service.rerank.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.RerankService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import io.stargate.sgv2.jsonapi.service.rerank.gateway.RerankEGWClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class RerankProviderFactory {
  @Inject RerankProvidersConfig rerankConfig;
  @Inject OperationsConfig operationsConfig;

  @GrpcClient("embedding")
  RerankService rerankGrpcService;

  interface ProviderConstructor {
    RerankProvider create(
        String baseUrl,
        String modelName,
        RerankProvidersConfig.RerankProviderConfig.ModelConfig.RequestProperties requestProperties);
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
    RerankProviderFactory.ProviderConstructor ctor =
        RERANK_PROVIDER_CONSTRUCTOR_MAP.get(serviceName);
    if (ctor == null) {
      throw ErrorCodeV1.RERANK_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", serviceName);
    }
    var modelConfig =
        configuration.models().stream()
            .filter(model -> model.name().equals(modelName))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.RERANK_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown model name '%s'", modelName));

    if (operationsConfig.enableEmbeddingGateway()) {
      // return the rerank Grpc client to embedding gateway service
      return new RerankEGWClient(
          modelConfig.url(),
          modelConfig.properties(),
          serviceName,
          tenant,
          authToken,
          modelName,
          rerankGrpcService,
          authentication,
          commandName);
    }

    return ctor.create(modelConfig.url(), modelConfig.name(), modelConfig.properties());
  }
}
