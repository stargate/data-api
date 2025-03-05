package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import io.quarkus.runtime.Startup;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.temporal.ChronoUnit;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RerankProviderConfigProducer {
  private static final Logger LOG = LoggerFactory.getLogger(RerankProviderConfigProducer.class);

  @Produces
  @ApplicationScoped
  @Startup
  @Retry(
      maxRetries = 30,
      delay = 6,
      delayUnit = ChronoUnit.SECONDS,
      maxDuration = 180,
      durationUnit = ChronoUnit.SECONDS)
  RerankProvidersConfig produce(
      DefaultRerankProviderConfig defaultRerankProviderConfig, OperationsConfig operationsConfig) {
    RerankProvidersConfig defaultConfig =
        new RerankProvidersConfigImpl(defaultRerankProviderConfig.providers());
    // defaultRerankProviderConfig is what we mapped from rerank-providers-config.yaml
    // and will be used if embedding-gateway is not enabled
    if (!operationsConfig.enableEmbeddingGateway()) {
      LOG.info("embedding gateway disabled, use default rerank config");
      return defaultConfig;
    }
    // TODO(Yuqi): add egw support
    LOG.info("embedding gateway enabled, fetch supported providers from embedding gateway");
    return defaultConfig;
  }
}
