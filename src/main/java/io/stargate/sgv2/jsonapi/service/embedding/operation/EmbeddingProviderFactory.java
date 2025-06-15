package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.gateway.EmbeddingGatewayClient;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class EmbeddingProviderFactory {

  @Inject Instance<EmbeddingProviderConfigStore> embeddingProviderConfigStore;
  @Inject EmbeddingProvidersConfig embeddingProvidersConfig;
  @Inject OperationsConfig operationsConfig;

  @GrpcClient("embedding")
  EmbeddingService grpcGatewayClient;

  @FunctionalInterface
  interface ProviderConstructor {
    EmbeddingProvider create(
        EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
        String baseUrl,
        EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
        int dimension,
        Map<String, Object> vectorizeServiceParameter);
  }

  // Immutable map, not concurrency concerns.
  private static final Map<ModelProvider, ProviderConstructor> EMBEDDING_PROVIDER_CTORS =
      Map.ofEntries(
          Map.entry(ModelProvider.AZURE_OPENAI, AzureOpenAIEmbeddingProvider::new),
          Map.entry(ModelProvider.BEDROCK, AwsBedrockEmbeddingProvider::new),
          Map.entry(ModelProvider.COHERE, CohereEmbeddingProvider::new),
          Map.entry(ModelProvider.HUGGINGFACE, HuggingFaceEmbeddingProvider::new),
          Map.entry(
              ModelProvider.HUGGINGFACE_DEDICATED, HuggingFaceDedicatedEmbeddingProvider::new),
          Map.entry(ModelProvider.JINA_AI, JinaAIEmbeddingProvider::new),
          Map.entry(ModelProvider.MISTRAL, MistralEmbeddingProvider::new),
          Map.entry(ModelProvider.NVIDIA, NvidiaEmbeddingProvider::new),
          Map.entry(ModelProvider.OPENAI, OpenAIEmbeddingProvider::new),
          Map.entry(ModelProvider.UPSTAGE_AI, UpstageAIEmbeddingProvider::new),
          Map.entry(ModelProvider.VERTEXAI, VertexAIEmbeddingProvider::new),
          Map.entry(ModelProvider.VOYAGE_AI, VoyageAIEmbeddingProvider::new));

  public EmbeddingProvider create(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Map<String, String> authentication,
      String commandName) {

    // aaron 7 June 2025, the code previously threw this error when the name from the config was not
    // found in the code, but this is a serious error that should not happen, it should be more like
    // a IllegalState.
    var modelProvider =
        ModelProvider.fromApiName(serviceName)
            .orElseThrow(
                () ->
                    ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown service provider '%s'", serviceName));

    return create(
        tenant,
        authToken,
        modelProvider,
        modelName,
        dimension,
        vectorizeServiceParameters,
        authentication,
        commandName);
  }

  public EmbeddingProvider create(
      Optional<String> tenant,
      Optional<String> authToken,
      ModelProvider modelProvider,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Map<String, String> authentication,
      String commandName) {

    if (vectorizeServiceParameters == null) {
      vectorizeServiceParameters = Map.of();
    }

    // WARNING: aaron 15 june 2025, I really cannot understand the difference between
    // EmbeddingProviderConfigStore and EmbeddingProvidersConfig this may be wrong
    // EmbeddingProvidersConfig looks like it is made by EmbeddingProvidersConfigProducer and then
    // considers if the embedding gateway is in use

    EmbeddingProviderConfigStore.ServiceConfig serviceConfig =
        embeddingProviderConfigStore.get().getConfiguration(tenant, modelProvider.apiName());

    // leaving full types here because the names are very, very confusing
    EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig =
        embeddingProvidersConfig.providers().get(serviceConfig.serviceProvider());
    if (providerConfig == null) {
      throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", serviceConfig.serviceProvider());
    }

    EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig =
        embeddingProvidersConfig.providers().get(serviceConfig.serviceProvider()).models().stream()
            .filter(m -> m.name().equals(modelName))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown model '%s' for service provider '%s'",
                        modelName, serviceConfig.serviceProvider()));

    if (operationsConfig.enableEmbeddingGateway()) {
      return new EmbeddingGatewayClient(
          modelProvider,
          providerConfig,
          serviceConfig.baseUrl(),
          modelConfig,
          dimension,
          vectorizeServiceParameters,
          tenant,
          authToken,
          grpcGatewayClient,
          authentication,
          commandName);
    }

    if (serviceConfig.serviceProvider().equals(ModelProvider.CUSTOM.apiName())) {
      // CUSTOM is for test only, but we cannot really check that here
      Optional<Class<?>> clazz = serviceConfig.implementationClass();
      if (clazz.isEmpty()) {
        throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
            "custom class undefined");
      }

      try {
        return (EmbeddingProvider) clazz.get().getConstructor(int.class).newInstance(dimension);
      } catch (Exception e) {
        throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
            "custom class provided ('%s') does not resolve to EmbeddingProvider",
            clazz.get().getCanonicalName());
      }
    }

    var ctor = EMBEDDING_PROVIDER_CTORS.get(modelProvider);
    if (ctor == null) {
      throw new IllegalStateException(
          "ModelProvider does not have a constructor: " + modelProvider);
    }

    return ctor.create(
        providerConfig,
        serviceConfig.baseUrl(),
        modelConfig,
        dimension,
        vectorizeServiceParameters);
  }
}
