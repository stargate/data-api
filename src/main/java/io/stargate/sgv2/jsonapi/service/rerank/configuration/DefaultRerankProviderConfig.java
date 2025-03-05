package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import io.smallrye.config.ConfigMapping;
import jakarta.annotation.Nullable;
import java.util.Map;

@ConfigMapping(prefix = "stargate.jsonapi.rerank")
public interface DefaultRerankProviderConfig {
  @Nullable
  Map<String, RerankProvidersConfig.RerankProviderConfig> providers();
}
