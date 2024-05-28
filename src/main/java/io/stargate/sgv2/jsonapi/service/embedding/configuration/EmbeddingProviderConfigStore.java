package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import java.util.Optional;

public interface EmbeddingProviderConfigStore {

  record ServiceConfig(
      String serviceName,
      String serviceProvider,
      String baseUrl,
      // `implementationClass` is the custom class that implements the EmbeddingProvider interface
      Optional<Class<?>> implementationClass,
      RequestProperties requestConfiguration) {

    public static ServiceConfig provider(
        String serviceName,
        String serviceProvider,
        String baseUrl,
        RequestProperties requestConfiguration) {
      return new ServiceConfig(serviceName, serviceProvider, baseUrl, null, requestConfiguration);
    }

    public static ServiceConfig custom(Optional<Class<?>> implementationClass) {
      return new ServiceConfig(
          ProviderConstants.CUSTOM, ProviderConstants.CUSTOM, null, implementationClass, null);
    }
  }

  record RequestProperties(
      int maxRetries,
      int retryDelayInMillis,
      int timeoutInMillis,
      Optional<String> requestTypeQuery,
      Optional<String> requestTypeIndex,
      int batchSize) {
    public static RequestProperties of(
        int maxRetries,
        int retryDelayInMillis,
        int timeoutInMillis,
        Optional<String> requestTypeQuery,
        Optional<String> requestTypeIndex,
        int batchSize) {
      return new RequestProperties(
          maxRetries,
          retryDelayInMillis,
          timeoutInMillis,
          requestTypeQuery,
          requestTypeIndex,
          batchSize);
    }
  }

  void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig);

  ServiceConfig getConfiguration(Optional<String> tenant, String serviceName);
}
