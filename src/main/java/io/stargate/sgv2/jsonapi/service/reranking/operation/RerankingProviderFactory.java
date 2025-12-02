package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.gateway.RerankingEGWClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class RerankingProviderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RerankingProviderFactory.class);

  @Inject RerankingProvidersConfig rerankingConfig;
  @Inject OperationsConfig operationsConfig;

  @GrpcClient("embedding")
  RerankingService grpcGatewayService;

  @FunctionalInterface
  interface ProviderConstructor {
    RerankingProvider create(
        RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig);
  }

  private static final Map<ModelProvider, ProviderConstructor> RERANKING_PROVIDER_CTORS =
      Map.ofEntries(Map.entry(ModelProvider.NVIDIA, NvidiaRerankingProvider::new));

  public RerankingProvider create(
      Tenant tenant,
      String authToken,
      String serviceName,
      String modelName,
      Map<String, String> authentication,
      String commandName) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "create() - tenant: {}, serviceName: {}, modelName: {}, commandName: {}",
          tenant,
          serviceName,
          modelName,
          commandName);
    }

    var modelProvider =
        ModelProvider.fromApiName(serviceName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Unknown reranking service provider '%s'", serviceName)));
    return create(tenant, authToken, modelProvider, modelName, authentication, commandName);
  }

  private synchronized RerankingProvider create(
      Tenant tenant,
      String authToken,
      ModelProvider modelProvider,
      String modelName,
      Map<String, String> authentication,
      String commandName) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "create() - tenant: {}, modelProvider: {}, modelName: {}, commandName: {}",
          tenant,
          modelProvider,
          modelName,
          commandName);
    }

    var providerConfig = rerankingConfig.providers().get(modelProvider.apiName());
    if (providerConfig == null) {
      throw ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown reranking service provider '%s'", modelProvider.apiName());
    }

    var modelConfig =
        providerConfig.models().stream()
            .filter(model -> model.name().equals(modelName))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown model name '%s'", modelName));

    if (operationsConfig.enableEmbeddingGateway()) {
      // return the reranking Grpc client to embedding gateway service
      return new RerankingEGWClient(
          modelProvider,
          modelConfig,
          tenant,
          authToken,
          grpcGatewayService,
          authentication,
          commandName);
    }

    RerankingProviderFactory.ProviderConstructor ctor = RERANKING_PROVIDER_CTORS.get(modelProvider);
    if (ctor == null) {
      throw ErrorCodeV1.RERANKING_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", modelProvider.apiName());
    }
    return ctor.create(modelConfig);
  }

  public RerankingProvidersConfig getRerankingConfig() {
    return rerankingConfig;
  }
}
