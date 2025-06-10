package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
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

  private static final Map<ModelProvider, ProviderConstructor> RERANKING_PROVIDER_CTORS =
      Map.ofEntries(Map.entry(ModelProvider.NVIDIA, NvidiaRerankingProvider::new));

  public RerankingProvider getConfiguration(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      Map<String, String> authentication,
      String commandName) {

    var modelProvider =
        ModelProvider.fromApiName(serviceName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Unknown reranking service provider '%s'", serviceName)));
    return addService(tenant, authToken, modelProvider, modelName, authentication, commandName);
  }

  private synchronized RerankingProvider addService(
      Optional<String> tenant,
      Optional<String> authToken,
      ModelProvider modelProvider,
      String modelName,
      Map<String, String> authentication,
      String commandName) {

    var rerankingProvderConfig = rerankingConfig.providers().get(modelProvider.apiName());

    RerankingProviderFactory.ProviderConstructor ctor = RERANKING_PROVIDER_CTORS.get(modelProvider);
    if (ctor == null) {
      throw ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", modelProvider.apiName());
    }

    var modelConfig =
        rerankingProvderConfig.models().stream()
            .filter(model -> model.name().equals(modelName))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown model name '%s'", modelName));

    if (operationsConfig.enableEmbeddingGateway()) {
      // return the reranking Grpc client to embedding gateway service
      return new RerankingEGWClient(
          modelConfig.url(),
          modelConfig.properties(),
          modelProvider,
          tenant,
          authToken,
          modelName,
          rerankingGrpcService,
          authentication,
          commandName);
    }

    return ctor.create(modelConfig.url(), modelConfig.name(), modelConfig.properties());
  }

  public RerankingProvidersConfig getRerankingConfig() {
    return rerankingConfig;
  }
}
