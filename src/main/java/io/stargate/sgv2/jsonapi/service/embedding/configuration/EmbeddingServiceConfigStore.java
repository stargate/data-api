package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import java.util.Optional;

public interface EmbeddingServiceConfigStore {

  record ServiceConfig(
      String serviceName, String serviceProvider, String apiKey, String baseUrl, String className) {
    public static ServiceConfig provider(
        String serviceName, String serviceProvider, String apiKey, String baseUrl) {
      return new ServiceConfig(serviceName, serviceProvider, apiKey, baseUrl, null);
    }

    public static ServiceConfig custom(String className) {
      return new ServiceConfig("custom", "custom", null, null, className);
    }
  }

  void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig);

  ServiceConfig getConfiguration(Optional<String> tenant, String serviceName);
}
