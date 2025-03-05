package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import jakarta.inject.Inject;
import java.util.Optional;

public class PropertyBasedRerankProviderConfigStore implements RerankProviderConfigStore {

  @Inject private DefaultRerankProviderConfig config;

  @Override
  public RerankProviderConfigStore.ServiceConfig getConfiguration(
      Optional<String> tenant, String serviceName) {
    if (config.providers().get(serviceName) == null
        || !config.providers().get(serviceName).enabled()) {
      throw ErrorCodeV1.RERANK_SERVICE_TYPE_UNAVAILABLE.toApiException(serviceName);
    }
    return RerankProviderConfigStore.ServiceConfig.provider(
        serviceName,
        config.providers().get(serviceName).models().get(0).url(),
        config.providers().get(serviceName).models().get(0).name());
  }
}
