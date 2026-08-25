package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the singleton {@link RerankingConcurrencyGate} for each provider+model pair.
 *
 * <p>{@link RerankingProviderFactory} constructs a fresh {@link RerankingProvider} (and REST
 * client) for every API request, so the shared per-pod concurrency state cannot live on the
 * provider itself — it is looked up here and handed to each new provider instance.
 */
@ApplicationScoped
public class RerankingConcurrencyGateRegistry {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RerankingConcurrencyGateRegistry.class);

  @Inject MeterRegistry meterRegistry;

  private final Map<String, RerankingConcurrencyGate> gates = new ConcurrentHashMap<>();

  public RerankingConcurrencyGate gateFor(
      ModelProvider modelProvider,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig) {
    return gates.computeIfAbsent(
        modelProvider.apiName() + "/" + modelConfig.name(),
        key -> createGate(modelProvider, modelConfig));
  }

  private RerankingConcurrencyGate createGate(
      ModelProvider modelProvider,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig) {
    var properties = modelConfig.properties();
    if (properties.totalTimeoutMillis() < properties.readTimeoutMillis()) {
      LOGGER.warn(
          "Reranking model {}/{} has total-timeout-millis {} below read-timeout-millis {}: "
              + "every call that reaches the read timeout will already have failed the total deadline",
          modelProvider.apiName(),
          modelConfig.name(),
          properties.totalTimeoutMillis(),
          properties.readTimeoutMillis());
    }
    LOGGER.info(
        "Creating reranking concurrency gate for {}/{}: maxConcurrentCalls={}, maxQueuedCalls={}",
        modelProvider.apiName(),
        modelConfig.name(),
        properties.maxConcurrentCalls(),
        properties.maxQueuedCalls());
    return new RerankingConcurrencyGate(
        modelProvider.apiName(),
        modelConfig.name(),
        properties.maxConcurrentCalls(),
        properties.maxQueuedCalls(),
        meterRegistry);
  }
}
