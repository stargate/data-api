package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import java.util.Optional;

public interface EmbeddingProviderConfigStore {

  record ServiceConfig(
      String serviceName,
      String serviceProvider,
      String apiKey,
      String baseUrl,
      Optional<Class<?>> clazz) {
    public static ServiceConfig provider(
        String serviceName, String serviceProvider, String apiKey, String baseUrl) {
      return new ServiceConfig(serviceName, serviceProvider, apiKey, baseUrl, null);
    }

    public static ServiceConfig custom(Optional<Class<?>> clazz) {
      return new ServiceConfig(
          ProviderConstants.CUSTOM, ProviderConstants.CUSTOM, null, null, clazz);
    }
  }

  void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig);

  ServiceConfig getConfiguration(Optional<String> tenant, String serviceName);
}
