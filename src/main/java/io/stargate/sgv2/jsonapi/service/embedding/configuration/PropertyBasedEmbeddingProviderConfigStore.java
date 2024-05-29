package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class PropertyBasedEmbeddingProviderConfigStore implements EmbeddingProviderConfigStore {

  @Inject private EmbeddingProvidersConfig config;

  @Override
  public void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public EmbeddingProviderConfigStore.ServiceConfig getConfiguration(
      Optional<String> tenant, String serviceName) {
    // already checked if the service exists and enabled in CreateCollectionCommandResolver
    if (serviceName.equals(ProviderConstants.CUSTOM)) {
      return ServiceConfig.custom(config.custom().clazz());
    }
    if (config.providers().get(serviceName) == null
        || !config.providers().get(serviceName).enabled()) {
      throw ErrorCode.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(serviceName);
    }
    final var properties = config.providers().get(serviceName).properties();
    return ServiceConfig.provider(
        serviceName,
        serviceName,
        config.providers().get(serviceName).url().toString(),
        RequestProperties.of(
            properties.maxRetries(),
            properties.retryDelayMillis(),
            properties.requestTimeoutMillis(),
            properties.taskTypeRead(),
            properties.taskTypeStore(),
            properties.maxBatchSize()));
  }
}
