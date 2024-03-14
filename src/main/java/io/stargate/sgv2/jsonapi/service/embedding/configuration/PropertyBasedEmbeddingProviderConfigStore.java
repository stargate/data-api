package io.stargate.sgv2.jsonapi.service.embedding.configuration;

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

  @Override
  public EmbeddingProviderConfigStore.ServiceConfig getConfiguration(
      Optional<String> tenant, String serviceName) {
    // already checked if the service exists and enabled in CreatCollectionCommandResolver
    if (serviceName.equals(ProviderConstants.CUSTOM)) {
      return ServiceConfig.custom(config.custom().clazz());
    }
    return ServiceConfig.provider(
        serviceName,
        serviceName,
        config.providers().get(serviceName).apiKey(),
        config.providers().get(serviceName).url().toString());
  }
}
