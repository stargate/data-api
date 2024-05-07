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
import org.slf4j.Logger;

@ApplicationScoped
public class EmbeddingProviderFactory {

  private static Logger logger = org.slf4j.LoggerFactory.getLogger(EmbeddingProviderFactory.class);
  @Inject Instance<EmbeddingProviderConfigStore> embeddingProviderConfigStore;

  @Inject OperationsConfig config;

  @GrpcClient("embedding")
  EmbeddingService embeddingService;

  private interface ProviderConstructor {
    EmbeddingProvider create(
        EmbeddingProviderConfigStore.RequestProperties requestProperties,
        String baseUrl,
        String modelName,
        int dimension,
        Map<String, Object> vectorizeServiceParameter);
  }

  private static final Map<String, ProviderConstructor> providersMap =
      Map.ofEntries(
          Map.entry(ProviderConstants.OPENAI, OpenAIEmbeddingClient::new),
          Map.entry(ProviderConstants.AZURE_OPENAI, AzureOpenAIEmbeddingClient::new),
          Map.entry(ProviderConstants.HUGGINGFACE, HuggingFaceEmbeddingClient::new),
          Map.entry(ProviderConstants.VERTEXAI, VertexAIEmbeddingClient::new),
          Map.entry(ProviderConstants.COHERE, CohereEmbeddingClient::new),
          Map.entry(ProviderConstants.NVIDIA, NvidiaEmbeddingClient::new));

  public EmbeddingProvider getConfiguration(
      Optional<String> tenant,
      String serviceName,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameter,
      String commandName) {
    return addService(
        tenant, serviceName, modelName, dimension, vectorizeServiceParameter, commandName);
  }

  private synchronized EmbeddingProvider addService(
      Optional<String> tenant,
      String serviceName,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameter,
      String commandName) {
    final EmbeddingProviderConfigStore.ServiceConfig configuration =
        embeddingProviderConfigStore.get().getConfiguration(tenant, serviceName);
    if (config.enableEmbeddingGateway()) {
      return new EmbeddingGatewayClient(
          configuration.requestConfiguration(),
          configuration.serviceProvider(),
          dimension,
          tenant,
          configuration.baseUrl(),
          modelName,
          embeddingService,
          vectorizeServiceParameter,
          commandName);
    }

    if (configuration.serviceProvider().equals(ProviderConstants.CUSTOM)) {
      try {
        Optional<Class<?>> clazz = configuration.implementationClass();
        if (clazz.isPresent()) {
          final EmbeddingProvider customEmbeddingProvider =
              (EmbeddingProvider) clazz.get().getConstructor().newInstance();
          return customEmbeddingProvider;
        } else {
          throw new JsonApiException(
              ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
              ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage() + "custom class undefined");
        }
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
            vectorizeServiceParameter);
  }
}
