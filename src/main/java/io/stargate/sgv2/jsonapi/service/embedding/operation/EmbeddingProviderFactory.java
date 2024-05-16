package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.EmbeddingService;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
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

  @Inject OperationsConfig config;

  @GrpcClient("embedding")
  EmbeddingService embeddingService;

  interface ProviderConstructor {
    EmbeddingProvider create(
        EmbeddingProviderConfigStore.RequestProperties requestProperties,
        String baseUrl,
        String modelName,
        int dimension,
        Map<String, Object> vectorizeServiceParameter);
  }

  private static final Map<String, ProviderConstructor> providersMap =
      // alphabetic order
      Map.ofEntries(
          Map.entry(ProviderConstants.AZURE_OPENAI, AzureOpenAIEmbeddingClient::new),
          Map.entry(ProviderConstants.COHERE, CohereEmbeddingClient::new),
          Map.entry(ProviderConstants.HUGGINGFACE, HuggingFaceEmbeddingClient::new),
          Map.entry(ProviderConstants.JINA_AI, JinaAIEmbeddingClient::new),
          Map.entry(ProviderConstants.MISTRAL, MistralEmbeddingClient::new),
          Map.entry(ProviderConstants.NVIDIA, NvidiaEmbeddingClient::new),
          Map.entry(ProviderConstants.OPENAI, OpenAIEmbeddingClient::new),
          Map.entry(ProviderConstants.UPSTAGE_AI, UpstageAIEmbeddingClient::new),
          Map.entry(ProviderConstants.VERTEXAI, VertexAIEmbeddingClient::new),
          Map.entry(ProviderConstants.VOYAGE_AI, VoyageAIEmbeddingClient::new));

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
          configuration.baseUrl(),
          modelName,
          embeddingService,
          vectorizeServiceParameters,
          authentication,
          commandName);
    }

    if (configuration.serviceProvider().equals(ProviderConstants.CUSTOM)) {
      Optional<Class<?>> clazz = configuration.implementationClass();
      if (!clazz.isPresent()) {
        throw new JsonApiException(
                ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
                ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage() + "custom class undefined");
      }
      try {
          return (EmbeddingProvider) clazz.get().getConstructor().newInstance();
      } catch (Exception e) {
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage()
                + "custom class provided does not resolve to EmbeddingProvider "
                + configuration.implementationClass().get().getCanonicalName());
      }
    }

    return providersMap
        .get(configuration.serviceProvider())
        .create(
            configuration.requestConfiguration(),
            configuration.baseUrl(),
            modelName,
            dimension,
            vectorizeServiceParameters);
  }
}
