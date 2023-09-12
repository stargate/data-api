package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class PropertyBasedEmbeddingServiceConfigStore implements EmbeddingServiceConfigStore {

  private PropertyBasedEmbeddingServiceConfig config;

  @Inject
  private PropertyBasedEmbeddingServiceConfigStore(PropertyBasedEmbeddingServiceConfig config) {
    this.config = config;
  }

  public static final PropertyBasedEmbeddingServiceConfigStore INSTANCE =
      new PropertyBasedEmbeddingServiceConfigStore(null);

  @Override
  public void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public EmbeddingServiceConfigStore.ServiceConfig getConfiguration(
      Optional<String> tenant, String serviceName) {
    if (config.apiType() == null || config.apiKey() == null || config.apiUrl() == null) {
      throw new RuntimeException("Missing configuration for embedding service");
    }
    return new ServiceConfig(config.apiType(), config.apiType(), config.apiKey(), config.apiUrl());
  }
}
