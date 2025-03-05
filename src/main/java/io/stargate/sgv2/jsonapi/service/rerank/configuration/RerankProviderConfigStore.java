package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import java.util.Optional;

public interface RerankProviderConfigStore {

  record ServiceConfig(String provider, String baseUrl, String modelName) {

    public static ServiceConfig provider(String provider, String baseUrl, String modelName) {
      return new ServiceConfig(provider, baseUrl, modelName);
    }
  }

  ServiceConfig getConfiguration(Optional<String> tenant, String serviceName);
}
