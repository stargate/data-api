package io.stargate.sgv2.jsonapi.service.provider.reranking.configuration;

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
        String name, boolean isDefault, String url, RequestProperties properties)
        implements ModelConfig {

      public record RequestPropertiesImpl(
          int atMostRetries,
          int initialBackOffMillis,
          int readTimeoutMillis,
          int maxBackOffMillis,
          double jitter,
          int maxBatchSize)
          implements RerankingProviderConfig.ModelConfig.RequestProperties {}
    }
  }
}
