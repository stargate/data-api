package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import javax.annotation.Nullable;

@ConfigMapping(prefix = "stargate.jsonapi.embedding.service")
public interface PropertyBasedEmbeddingServiceConfig {

  @Nullable
  OpenaiConfig openai();

  @Nullable
  HuggingFaceConfig hf();

  @Nullable
  VertexAiConfig vertexai();

  public interface OpenaiConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://api.openai.com/v1")
    String url();

    @WithDefault("Bearer")
    String apiKey();

    @WithDefault("davinci")
    String modelName();
  }

  public interface HuggingFaceConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://api-inference.huggingface.co")
    String url();

    @WithDefault("Bearer")
    String apiKey();

    @WithDefault("davinci")
    String modelName();
  }

  public interface VertexAiConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://us-central1-aiplatform.googleapis.com/v1")
    String url();

    @WithDefault("Bearer")
    String apiKey();

    @WithDefault("davinci")
    String modelName();
  }
}
