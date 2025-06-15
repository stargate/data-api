package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * aaron - 16 june 2025 - I think this is the configuration taking into account the definition of
 * the service in the schema.
 */
public interface ServiceConfigStore {

  record ServiceConfig(
      ModelProvider modelProvider,
      // aaron 16 june 2025 - danger here, there is a method that changes the baseUrl use
      // getBaseUrl()
      // will refactor later
      String baseUrl,
      // `implementationClass` is the custom class that implements the EmbeddingProvider interface
      Optional<Class<?>> implementationClass,
      ServiceRequestProperties requestProperties,
      Map<String, Optional<String>> modelUrlOverrides) {

    public static ServiceConfig forKnownProvider(
        ModelProvider modelProvider,
        String baseUrl,
        ServiceRequestProperties requestConfiguration,
        Map<String, Optional<String>> modelUrlOverrides) {

      return new ServiceConfig(
          modelProvider, baseUrl, Optional.empty(), requestConfiguration, modelUrlOverrides);
    }

    public static ServiceConfig forCustomProvider(Class<?> implementationClass) {
      Objects.requireNonNull(implementationClass, "implementationClass must not be null");

      return new ServiceConfig(
          ModelProvider.CUSTOM, null, Optional.of(implementationClass), null, Map.of());
    }

    public String getBaseUrl(String modelName) {

      if (modelUrlOverrides != null && modelUrlOverrides.get(modelName) == null) {
        // modelUrlOverride is a work-around for self-hosted nvidia models with different url.
        // This is bad, initial design should have url in model level instead of provider level.
        // As best practice, when we deprecate or EOL a model:
        // we must mark the status in the configuration,
        // instead of removing the whole configuration entry.
        throw ErrorCodeV1.VECTORIZE_SERVICE_TYPE_UNAVAILABLE.toApiException(
            "unknown model '%s' for service provider '%s'", modelName, modelProvider);
      }
      return modelUrlOverrides != null ? modelUrlOverrides.get(modelName).orElse(baseUrl) : baseUrl;
    }
  }

  record ServiceRequestProperties(
      int atMostRetries,
      int initialBackOffMillis,
      int readTimeoutMillis,
      int maxBackOffMillis,
      double jitter,
      Optional<String> requestTypeQuery,
      Optional<String> requestTypeIndex,
      // `maxBatchSize` is the maximum number of documents to be sent in a single request to be
      // embedding provider
      int maxBatchSize) {}

  //  void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig);

  ServiceConfig getConfiguration(ModelProvider modelProvider);
}
