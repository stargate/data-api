package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@ApplicationScoped
public class PropertyBasedEmbeddingProviderConfigStore implements EmbeddingProviderConfigStore {

  @Inject private EmbeddingProvidersConfig config;

  @Override
  public void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig) {
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "PropertyBasedEmbeddingProviderConfigStore.saveConfiguration() not implemented");
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
      throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(serviceName);
    }
    final var properties = config.providers().get(serviceName).properties();
    Map<String, Optional<String>> modelwiseServiceUrlOverrides =
        Objects.requireNonNull(config.providers().get(serviceName).models()).stream()
            .collect(
                HashMap::new,
                (map, modelConfig) -> map.put(modelConfig.name(), modelConfig.serviceUrlOverride()),
                HashMap::putAll);
    return ServiceConfig.provider(
        serviceName,
        serviceName,
        config.providers().get(serviceName).url().orElse(null),
        RequestProperties.of(
            properties.atMostRetries(),
            properties.initialBackOffMillis(),
            properties.readTimeoutMillis(),
            properties.maxBackOffMillis(),
            properties.jitter(),
            properties.taskTypeRead(),
            properties.taskTypeStore(),
            properties.maxBatchSize()),
        modelwiseServiceUrlOverrides);
  }
}
