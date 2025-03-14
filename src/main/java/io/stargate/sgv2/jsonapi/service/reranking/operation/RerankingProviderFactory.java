package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.gateway.RerankingEGWClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class RerankingProviderFactory {
  @Inject RerankingProvidersConfig rerankingConfig;
  @Inject OperationsConfig operationsConfig;

  @GrpcClient("embedding")
  RerankingService rerankingGrpcService;

  interface ProviderConstructor {
    RerankingProvider create(
        String baseUrl,
        String modelName,
        RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
            requestProperties);
  }

  private static final Map<String, ProviderConstructor> RERANK_PROVIDER_CONSTRUCTOR_MAP =
      Map.ofEntries(Map.entry("nvidia", NvidiaRerankingProvider::new));

  public RerankingProvider getConfiguration(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      Map<String, String> authentication,
      String commandName) {
    return addService(tenant, authToken, serviceName, modelName, authentication, commandName);
  }

  private synchronized RerankingProvider addService(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      Map<String, String> authentication,
      String commandName) {
    final RerankingProvidersConfig.RerankingProviderConfig configuration =
        rerankingConfig.providers().get(serviceName);
    RerankingProviderFactory.ProviderConstructor ctor =
        RERANK_PROVIDER_CONSTRUCTOR_MAP.get(serviceName);
    if (ctor == null) {
      throw ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", serviceName);
    }
    var modelConfig =
        configuration.models().stream()
            .filter(model -> model.name().equals(modelName))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown model name '%s'", modelName));

    if (operationsConfig.enableEmbeddingGateway()) {
      // return the rerank Grpc client to embedding gateway service
      return new RerankingEGWClient(
          modelConfig.url(),
          modelConfig.properties(),
          serviceName,
          tenant,
          authToken,
          modelName,
          rerankingGrpcService,
          authentication,
          commandName);
    }

    return ctor.create(modelConfig.url(), modelConfig.name(), modelConfig.properties());
  }
}
