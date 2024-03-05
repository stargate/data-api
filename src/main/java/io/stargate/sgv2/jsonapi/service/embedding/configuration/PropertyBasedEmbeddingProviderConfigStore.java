package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class PropertyBasedEmbeddingProviderConfigStore implements EmbeddingProviderConfigStore {

  @Inject private PropertyBasedEmbeddingProviderConfig config;

  @Override
  public void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public EmbeddingProviderConfigStore.ServiceConfig getConfiguration(
      Optional<String> tenant, String serviceName) {
    switch (serviceName) {
      case "openai":
        if (config.openai().enabled()) {
          return ServiceConfig.provider(
              serviceName, serviceName, config.openai().apiKey(), config.openai().url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "huggingface":
        if (config.huggingface().enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.huggingface().apiKey(),
              config.huggingface().url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "vertexai":
        if (config.vertexai().enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.vertexai().apiKey(),
              config.vertexai().url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "cohere":
        if (config.cohere().enabled()) {
          return ServiceConfig.provider(
              serviceName, serviceName, config.cohere().apiKey(), config.cohere().url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "nvidia":
        if (config.nvidia().enabled()) {
          return ServiceConfig.provider(
              serviceName, serviceName, config.nvidia().apiKey(), config.nvidia().url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "custom":
        if (config.custom().enabled()) {
          return ServiceConfig.custom(config.custom().clazz());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      default:
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
    }
  }
}
