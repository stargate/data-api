package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import java.util.Map;
import java.util.Optional;

public interface EmbeddingProviderConfigStore {

  record ServiceConfig(
      String serviceName,
      String serviceProvider,
      String baseUrl,
      // `implementationClass` is the custom class that implements the EmbeddingProvider interface
      Optional<Class<?>> implementationClass,
      RequestProperties requestConfiguration,
      Map<String, Optional<String>> modelUrlOverrides) {

    public static ServiceConfig provider(
        String serviceName,
        String serviceProvider,
        String baseUrl,
        RequestProperties requestConfiguration,
        Map<String, Optional<String>> modelUrlOverrides) {
      return new ServiceConfig(
          serviceName, serviceProvider, baseUrl, null, requestConfiguration, modelUrlOverrides);
    }

    public static ServiceConfig custom(Optional<Class<?>> implementationClass) {
      return new ServiceConfig(
          ProviderConstants.CUSTOM,
          ProviderConstants.CUSTOM,
          null,
          implementationClass,
          null,
          Map.of());
    }

    public String getBaseUrl(String modelName) {
      return modelUrlOverrides != null ? modelUrlOverrides.get(modelName).orElse(baseUrl) : baseUrl;
    }
  }

  record RequestProperties(
      int atMostRetries,
      int initialBackOffMillis,
      int readTimeoutMillis,
      int maxBackOffMillis,
      double jitter,
      Optional<String> requestTypeQuery,
      Optional<String> requestTypeIndex,
      // `maxBatchSize` is the maximum number of documents to be sent in a single request to be
      // embedding provider
      int maxBatchSize) {
    public static RequestProperties of(
        int atMostRetries,
        int initialBackOffMillis,
        int readTimeoutMillis,
        int maxBackOffMillis,
        double jitter,
        Optional<String> requestTypeQuery,
        Optional<String> requestTypeIndex,
        int maxBatchSize) {
      return new RequestProperties(
          atMostRetries,
          initialBackOffMillis,
          readTimeoutMillis,
          maxBackOffMillis,
          jitter,
          requestTypeQuery,
          requestTypeIndex,
          maxBatchSize);
    }
  }

  void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig);

  ServiceConfig getConfiguration(Optional<String> tenant, String serviceName);
}
