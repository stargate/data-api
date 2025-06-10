package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
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

  @Inject OperationsConfig operationsConfig;

  @GrpcClient("embedding")
  EmbeddingService embeddingService;

  @FunctionalInterface
  interface ProviderConstructor {
    EmbeddingProvider create(
        EmbeddingProviderConfigStore.RequestProperties requestProperties,
        String baseUrl,
        String modelName,
        int dimension,
        Map<String, Object> vectorizeServiceParameter);
  }

  private static final Map<ModelProvider, ProviderConstructor> EMBEDDING_PROVIDER_CTORS =
      // alphabetic order
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

  public EmbeddingProvider getConfiguration(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Map<String, String> authentication,
      String commandName) {

    if (vectorizeServiceParameters == null) {
      vectorizeServiceParameters = Map.of();
    }

    var modelProvider =
        ModelProvider.fromApiName(serviceName)
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown service provider: " + serviceName));

    return addService(
        tenant,
        authToken,
        modelProvider,
        modelName,
        dimension,
        vectorizeServiceParameters,
        authentication,
        commandName);
  }

  private synchronized EmbeddingProvider addService(
      Optional<String> tenant,
      Optional<String> authToken,
      ModelProvider modelProvider,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Map<String, String> authentication,
      String commandName) {

    final EmbeddingProviderConfigStore.ServiceConfig serviceConfig =
        embeddingProviderConfigStore.get().getConfiguration(tenant, modelProvider.apiName());

    if (operationsConfig.enableEmbeddingGateway()) {
      return new EmbeddingGatewayClient(
          serviceConfig.requestConfiguration(),
          modelProvider,
          dimension,
          tenant,
          authToken,
          serviceConfig.getBaseUrl(modelName),
          modelName,
          embeddingService,
          vectorizeServiceParameters,
          authentication,
          commandName);
    }

    if (serviceConfig.serviceProvider().equals(ModelProvider.CUSTOM.apiName())) {
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

    // aaron 7 June 2025, the code previously threw this error when the name from the config was not
    // found in the code, but this is a serious error that should not happen, it should be more like
    // a IllegalState.
    var serviceConfigModelProvider =
        ModelProvider.fromApiName(serviceConfig.serviceProvider())
            .orElseThrow(
                () ->
                    ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown service provider '%s'", serviceConfig.serviceProvider()));

    ProviderConstructor ctor = EMBEDDING_PROVIDER_CTORS.get(serviceConfigModelProvider);
    if (ctor == null) {
      throw new IllegalStateException(
          "ModelProvider does not have a constructor: " + serviceConfigModelProvider.apiName());
    }

    return ctor.create(
        serviceConfig.requestConfiguration(),
        serviceConfig.getBaseUrl(modelName),
        modelName,
        dimension,
        vectorizeServiceParameters);
  }
}
