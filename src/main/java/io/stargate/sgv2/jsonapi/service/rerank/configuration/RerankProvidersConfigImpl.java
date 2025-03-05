package io.stargate.sgv2.jsonapi.service.rerank.configuration;

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

    public record AuthenticationConfigImpl(boolean enabled, List<TokenConfig> tokens)
        implements AuthenticationConfig {

      public record TokenConfigImpl(String accepted, String forwarded) implements TokenConfig {}
    }

    public record ModelConfigImpl(String name, String url, RequestProperties properties)
        implements ModelConfig {}
  }
}
