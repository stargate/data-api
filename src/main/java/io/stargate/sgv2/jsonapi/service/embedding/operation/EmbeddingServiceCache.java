package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingServiceConfigStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class EmbeddingServiceCache {
  @Inject EmbeddingServiceConfigStore embeddingServiceConfigStore;

  record CacheKey(Optional<String> tenant, String namespace) {}

  private final Cache<CacheKey, EmbeddingService> serviceConfigStore =
      Caffeine.newBuilder().maximumSize(1000).build();

  public EmbeddingService getConfiguration(
      Optional<String> tenant, String serviceName, String modelName) {
    EmbeddingService embeddingService =
        serviceConfigStore.getIfPresent(new CacheKey(tenant, serviceName));
    if (embeddingService == null) {
      embeddingService = addService(tenant, serviceName, modelName);
      serviceConfigStore.put(new CacheKey(tenant, serviceName), embeddingService);
    }
    return embeddingService;
  }

  private synchronized EmbeddingService addService(
      Optional<String> tenant, String serviceName, String modelName) {
    final EmbeddingServiceConfigStore.ServiceConfig configuration =
        embeddingServiceConfigStore.getConfiguration(tenant, serviceName);
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
      default:
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNSUPPORTED,
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNSUPPORTED.getMessage() + serviceName);
    }
  }
}
