package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * aaron - 17 june 2025 - as far as I can tell there is a single implementation of this interface,
 * not sure why this is an interface, why this class exists, and why it is ApplicationScoped.
 */
@ApplicationScoped
public class PropertyBasedServiceConfigStore implements ServiceConfigStore {

  @Inject private EmbeddingProvidersConfig providersConfig;

  @Override
  public ServiceConfigStore.ServiceConfig getConfiguration(ModelProvider modelProvider) {

    // already checked if the service exists and enabled in CreateCollectionCommandResolver
    if (modelProvider == ModelProvider.CUSTOM) {
      Objects.requireNonNull(
          providersConfig.custom(), "ModelProvider is CUSTOM configuration has null custom config");
      Objects.requireNonNull(
          providersConfig.custom().clazz(), "ModelProvider is CUSTOM configuration has null class");

      /**
       * See {@link DseTestResource} for where the implementationClass is set, and {@link
       * PropertyBasedServiceConfigStore} for where it is read
       */
      return ServiceConfig.forCustomProvider(
          providersConfig
              .custom()
              .clazz()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "ModelProvider is CUSTOM but no class is provided in configuration")));
    }

    var providerConfig = providersConfig.providers().get(modelProvider.apiName());
    if (providerConfig == null || !providerConfig.enabled()) {
      throw EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNAVAILABLE.get(
          "errorMessage",
          "The service provider '%s' is not supported.".formatted(modelProvider.apiName()));
    }

    Objects.requireNonNull(
        providerConfig.models(),
        "ModelProvider configuration has null models, provider: " + modelProvider.apiName());

    // aaron 16 June 2025 - not sure what this is doing, left in place for now
    Map<String, Optional<String>> modelwiseServiceUrlOverrides =
        providerConfig.models().stream()
            .collect(
                HashMap::new,
                (map, modelConfig) -> map.put(modelConfig.name(), modelConfig.serviceUrlOverride()),
                HashMap::putAll);

    var requestProperties = providerConfig.properties();
    return ServiceConfig.forKnownProvider(
        modelProvider,
        providerConfig.url().orElse(null),
        new ServiceRequestProperties(
            requestProperties.atMostRetries(),
            requestProperties.initialBackOffMillis(),
            requestProperties.readTimeoutMillis(),
            requestProperties.maxBackOffMillis(),
            requestProperties.jitter(),
            requestProperties.taskTypeRead(),
            requestProperties.taskTypeStore(),
            requestProperties.maxBatchSize()),
        modelwiseServiceUrlOverrides);
  }
}
