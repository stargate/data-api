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
        if (config.providers().get("openai").enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.providers().get("openai").apiKey(),
              config.providers().get("openai").url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "huggingface":
        if (config.providers().get("huggingface").enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.providers().get("huggingface").apiKey(),
              config.providers().get("huggingface").url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "vertexai":
        if (config.providers().get("vertexai").enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.providers().get("vertexai").apiKey(),
              config.providers().get("vertexai").url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "cohere":
        if (config.providers().get("cohere").enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.providers().get("cohere").apiKey(),
              config.providers().get("cohere").url().toString());
        }
        throw ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.toApiException(serviceName);
      case "nvidia":
        if (config.providers().get("nvidia").enabled()) {
          return ServiceConfig.provider(
              serviceName,
              serviceName,
              config.providers().get("nvidia").apiKey(),
              config.providers().get("nvidia").url().toString());
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
