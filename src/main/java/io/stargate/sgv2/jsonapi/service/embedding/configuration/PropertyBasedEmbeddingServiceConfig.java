package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.smallrye.config.ConfigMapping;
import jakarta.validation.constraints.Pattern;
import javax.annotation.Nullable;

@ConfigMapping(prefix = "stargate.jsonapi.embedding.service")
public interface PropertyBasedEmbeddingServiceConfig {
  @Nullable
  @Pattern(regexp = "openai|vertexai|huggingface", message = "Unsupported api type")
  String apiType();

  @Nullable
  String apiUrl();

  @Nullable
  String apiKey();

  @Nullable
  String modelName();
}
