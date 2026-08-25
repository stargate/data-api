package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import java.util.List;
import java.util.Map;

public record RerankingProvidersConfigImpl(Map<String, RerankingProviderConfig> providers)
    implements RerankingProvidersConfig {

  public record RerankingProviderConfigImpl(
      boolean isDefault,
      String displayName,
      boolean enabled,
      Map<AuthenticationType, AuthenticationConfig> supportedAuthentications,
      List<ModelConfig> models)
      implements RerankingProviderConfig {

    public record AuthenticationConfigImpl(boolean enabled, List<TokenConfig> tokens)
        implements AuthenticationConfig {

      public record TokenConfigImpl(String accepted, String forwarded) implements TokenConfig {}
    }

    public record ModelConfigImpl(
        String name,
        ApiModelSupport apiModelSupport,
        boolean isDefault,
        String url,
        RequestProperties properties)
        implements ModelConfig {

      public record RequestPropertiesImpl(
          int atMostRetries,
          int initialBackOffMillis,
          int readTimeoutMillis,
          int maxBackOffMillis,
          double jitter,
          int maxBatchSize,
          int maxConcurrentBatches,
          int maxConcurrentCalls,
          int maxQueuedCalls,
          int totalTimeoutMillis)
          implements RerankingProviderConfig.ModelConfig.RequestProperties {

        /** Convenience constructor applying the concurrency-gate defaults. */
        public RequestPropertiesImpl(
            int atMostRetries,
            int initialBackOffMillis,
            int readTimeoutMillis,
            int maxBackOffMillis,
            double jitter,
            int maxBatchSize) {
          this(
              atMostRetries,
              initialBackOffMillis,
              readTimeoutMillis,
              maxBackOffMillis,
              jitter,
              maxBatchSize,
              Integer.parseInt(
                  RerankingProviderConfig.ModelConfig.RequestProperties
                      .DEFAULT_MAX_CONCURRENT_BATCHES),
              Integer.parseInt(
                  RerankingProviderConfig.ModelConfig.RequestProperties
                      .DEFAULT_MAX_CONCURRENT_CALLS),
              Integer.parseInt(
                  RerankingProviderConfig.ModelConfig.RequestProperties.DEFAULT_MAX_QUEUED_CALLS),
              Integer.parseInt(
                  RerankingProviderConfig.ModelConfig.RequestProperties
                      .DEFAULT_TOTAL_TIMEOUT_MILLIS));
        }
      }
    }
  }
}
