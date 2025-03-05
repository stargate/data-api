package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RerankProvidersConfig {
  Map<String, RerankProviderConfig> providers();

  interface RerankProviderConfig {
    @JsonProperty
    String displayName();

    @JsonProperty
    boolean enabled();

    @Nullable
    @JsonProperty
    Optional<String> url();

    @JsonProperty
    Map<AuthenticationType, AuthenticationConfig> supportedAuthentications();

    enum AuthenticationType {
      NONE,
      HEADER,
      SHARED_SECRET
    }

    interface AuthenticationConfig {
      @JsonProperty
      boolean enabled();

      @JsonProperty
      List<TokenConfig> tokens();
    }

    interface TokenConfig {
      @JsonProperty
      String accepted();

      @JsonProperty
      String forwarded();
    }

    //    @Nullable
    @JsonProperty
    List<ModelConfig> models();

    interface ModelConfig {
      @JsonProperty
      String name();

      @Nullable
      @JsonProperty
      Optional<String> url();
    }
  }
}
