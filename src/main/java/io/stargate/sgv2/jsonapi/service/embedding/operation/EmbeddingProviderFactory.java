package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Optional;
import org.slf4j.Logger;

@ApplicationScoped
public class EmbeddingProviderFactory {

  private static Logger logger = org.slf4j.LoggerFactory.getLogger(EmbeddingProviderFactory.class);
  @Inject Instance<EmbeddingProviderConfigStore> embeddingProviderConfigStore;

  record CacheKey(Optional<String> tenant, String namespace, String modelName) {}

  public EmbeddingProvider getConfiguration(
      Optional<String> tenant, String serviceName, String modelName) {
    return addService(tenant, serviceName, modelName);
  }

  private synchronized EmbeddingProvider addService(
      Optional<String> tenant, String serviceName, String modelName) {
    final EmbeddingProviderConfigStore.ServiceConfig configuration =
        embeddingProviderConfigStore.get().getConfiguration(tenant, serviceName);
    if (configuration == null) {
      throw new JsonApiException(
          ErrorCode.VECTORIZE_SERVICE_NOT_REGISTERED,
          ErrorCode.VECTORIZE_SERVICE_NOT_REGISTERED.getMessage() + serviceName);
    }
    EmbeddingProvider service;
    switch (configuration.serviceProvider()) {
      case "openai":
        return new OpenAiEmbeddingClient(
            configuration.baseUrl(), configuration.apiKey(), modelName);
      case "huggingface":
        return new HuggingFaceEmbeddingClient(
            configuration.baseUrl(), configuration.apiKey(), modelName);
      case "vertexai":
        return new VertexAIEmbeddingClient(
            configuration.baseUrl(), configuration.apiKey(), modelName);
      case "cohere":
        return new OpenAiEmbeddingClient(
            configuration.baseUrl(), configuration.apiKey(), modelName);
      case "nvidia":
        return new OpenAiEmbeddingClient(
            configuration.baseUrl(), configuration.apiKey(), modelName);
      case "custom":
        try {
          Optional<Class<?>> clazz = configuration.clazz();
          if (clazz.isPresent()) {
            final EmbeddingProvider customEmbeddingProvider =
                (EmbeddingProvider) clazz.get().getConstructor().newInstance();
            return customEmbeddingProvider;
          } else {
            throw new JsonApiException(
                ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
                ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage()
                    + "custom class undefined");
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw new JsonApiException(
              ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE,
              ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.getMessage()
                  + "custom class provided does not resolved to EmbeddingProvider "
                  + configuration.clazz().get().getCanonicalName());
        }
      default:
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(serviceName);
    }
  }
}
