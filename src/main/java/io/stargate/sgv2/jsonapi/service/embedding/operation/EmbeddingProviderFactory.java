package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.gateway.EmbeddingGatewayClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class EmbeddingProviderFactory {
  @Inject Instance<EmbeddingProviderConfigStore> embeddingProviderConfigStore;
  @Inject EmbeddingProvidersConfig embeddingProvidersConfig;
  @Inject OperationsConfig config;

  @GrpcClient("embedding")
  EmbeddingService embeddingService;

  interface ProviderConstructor {
    EmbeddingProvider create(
        EmbeddingProviderConfigStore.RequestProperties requestProperties,
        String baseUrl,
        EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
        int dimension,
        Map<String, Object> vectorizeServiceParameter);
  }

  private static final Map<String, ProviderConstructor> providersMap =
      // alphabetic order
      Map.ofEntries(
          Map.entry(ProviderConstants.AZURE_OPENAI, AzureOpenAIEmbeddingProvider::new),
          Map.entry(ProviderConstants.COHERE, CohereEmbeddingProvider::new),
          Map.entry(ProviderConstants.HUGGINGFACE, HuggingFaceEmbeddingProvider::new),
          Map.entry(
              ProviderConstants.HUGGINGFACE_DEDICATED, HuggingFaceDedicatedEmbeddingProvider::new),
          Map.entry(ProviderConstants.JINA_AI, JinaAIEmbeddingProvider::new),
          Map.entry(ProviderConstants.MISTRAL, MistralEmbeddingProvider::new),
          Map.entry(ProviderConstants.NVIDIA, NvidiaEmbeddingProvider::new),
          Map.entry(ProviderConstants.OPENAI, OpenAIEmbeddingProvider::new),
          Map.entry(ProviderConstants.UPSTAGE_AI, UpstageAIEmbeddingProvider::new),
          Map.entry(ProviderConstants.VERTEXAI, VertexAIEmbeddingProvider::new),
          Map.entry(ProviderConstants.VOYAGE_AI, VoyageAIEmbeddingProvider::new),
          Map.entry(ProviderConstants.BEDROCK, AwsBedrockEmbeddingProvider::new));

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
    return addService(
        tenant,
        authToken,
        serviceName,
        modelName,
        dimension,
        vectorizeServiceParameters,
        authentication,
        commandName);
  }

  private synchronized EmbeddingProvider addService(
      Optional<String> tenant,
      Optional<String> authToken,
      String serviceName,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Map<String, String> authentication,
      String commandName) {
    final EmbeddingProviderConfigStore.ServiceConfig configuration =
        embeddingProviderConfigStore.get().getConfiguration(tenant, serviceName);
    if (config.enableEmbeddingGateway()) {
      return new EmbeddingGatewayClient(
          configuration.requestConfiguration(),
          configuration.serviceProvider(),
          dimension,
          tenant,
          authToken,
          configuration.getBaseUrl(modelName),
          modelName,
          embeddingService,
          vectorizeServiceParameters,
          authentication,
          commandName);
    }

    if (configuration.serviceProvider().equals(ProviderConstants.CUSTOM)) {
      Optional<Class<?>> clazz = configuration.implementationClass();
      if (!clazz.isPresent()) {
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

    ProviderConstructor ctor = providersMap.get(configuration.serviceProvider());
    if (ctor == null) {
      throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", configuration.serviceProvider());
    }

    // Get the provider, then get the model.
    var providerConfig = embeddingProvidersConfig.providers().get(configuration.serviceProvider());
    if (providerConfig == null) {
      throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
          "unknown service provider '%s'", configuration.serviceProvider());
    }
    EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model =
        embeddingProvidersConfig.providers().get(configuration.serviceProvider()).models().stream()
            .filter(m -> m.name().equals(modelName))
            .findFirst()
            .orElseThrow(
                () ->
                    ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
                        "unknown model '%s' for service provider '%s'",
                        modelName, configuration.serviceProvider()));
    return ctor.create(
        configuration.requestConfiguration(),
        configuration.getBaseUrl(modelName),
        model,
        dimension,
        vectorizeServiceParameters);
  }
}
