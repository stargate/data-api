package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.smallrye.config.ConfigMapping;
import jakarta.annotation.Nullable;
import java.util.Map;

@ConfigMapping(prefix = "stargate.jsonapi.embedding")
public interface FileEmbeddingProviderConfig {
  @Nullable
  Map<String, EmbeddingProvidersConfig.EmbeddingProviderConfig> providers();
}
