package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingServiceConfigStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Optional;
import org.slf4j.Logger;

@ApplicationScoped
public class EmbeddingServiceCache {

  private static Logger logger = org.slf4j.LoggerFactory.getLogger(EmbeddingServiceCache.class);
  @Inject Instance<EmbeddingServiceConfigStore> embeddingServiceConfigStore;

  record CacheKey(Optional<String> tenant, String namespace, String modelName) {}

  private final Cache<CacheKey, EmbeddingService> serviceConfigStore =
      Caffeine.newBuilder().maximumSize(1000).build();

  public EmbeddingService getConfiguration(
      Optional<String> tenant, String serviceName, String modelName) {
    EmbeddingService embeddingService =
        serviceConfigStore.getIfPresent(new CacheKey(tenant, serviceName, modelName));
    if (embeddingService == null) {
      embeddingService = addService(tenant, serviceName, modelName);
      if (embeddingService != null)
        serviceConfigStore.put(new CacheKey(tenant, serviceName, modelName), embeddingService);
    }
    return embeddingService;
  }

  private synchronized EmbeddingService addService(
      Optional<String> tenant, String serviceName, String modelName) {
    final EmbeddingServiceConfigStore.ServiceConfig configuration =
        embeddingServiceConfigStore.get().getConfiguration(tenant, serviceName);
    if (configuration == null) {
      throw new JsonApiException(
          ErrorCode.VECTORIZE_SERVICE_NOT_REGISTERED,
          ErrorCode.VECTORIZE_SERVICE_NOT_REGISTERED.getMessage() + serviceName);
    }
    EmbeddingService service;
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
      case "custom":
        try {
          Optional<Class<?>> clazz = configuration.clazz();
          if (clazz.isPresent()) {
            final EmbeddingService customEmbeddingClient =
                (EmbeddingService) clazz.get().getConstructor().newInstance();
            return customEmbeddingClient;
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
                  + "custom class provided does not resolved to EmbeddingService "
                  + configuration.clazz().get().getCanonicalName());
        }
      default:
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNSUPPORTED,
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNSUPPORTED.getMessage() + serviceName);
    }
  }
}
