package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public record RerankProvidersConfigImpl(Map<String, RerankProviderConfig> providers)
    implements RerankProvidersConfig {

  public record RerankProviderConfigImpl(
      String displayName,
      boolean enabled,
      Optional<String> url,
      Map<AuthenticationType, AuthenticationConfig> supportedAuthentications,
      List<ModelConfig> models)
      implements RerankProviderConfig {

    public record AuthenticationConfigImpl(
        boolean enabled, List<EmbeddingProvidersConfig.EmbeddingProviderConfig.TokenConfig> tokens)
        implements EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig {

      public record TokenConfigImpl(String accepted, String forwarded)
          implements EmbeddingProvidersConfig.EmbeddingProviderConfig.TokenConfig {}
    }

    public record ModelConfigImpl(String name, Optional<String> url) implements ModelConfig {}
  }
}
