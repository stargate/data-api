package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import java.util.List;
import java.util.Map;

public record RerankProvidersConfigImpl(Map<String, RerankProviderConfig> providers)
    implements RerankProvidersConfig {

  public record RerankProviderConfigImpl(
      boolean isDefault,
      String displayName,
      boolean enabled,
      Map<AuthenticationType, AuthenticationConfig> supportedAuthentications,
      List<ModelConfig> models)
      implements RerankProviderConfig {

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
          implements RerankProvidersConfig.RerankProviderConfig.ModelConfig.RequestProperties {}
    }
  }
}
