package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import io.smallrye.config.ConfigMapping;
import jakarta.annotation.Nullable;
import java.util.Map;

@ConfigMapping(prefix = "stargate.jsonapi.reranking")
public interface DefaultRerankingProviderConfig {
  @Nullable
  Map<String, RerankingProvidersConfig.RerankingProviderConfig> providers();
}
