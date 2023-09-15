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

  @Nullable
  CustomConfig custom();

  public interface OpenaiConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://api.openai.com/v1")
    String url();

    @WithDefault("Bearer")
    String apiKey();
  }

  public interface HuggingFaceConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://api-inference.huggingface.co")
    String url();

    @WithDefault("Bearer")
    String apiKey();
  }

  public interface VertexAiConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://us-central1-aiplatform.googleapis.com/v1")
    String url();

    @WithDefault("Bearer")
    String apiKey();
  }

  public interface CustomConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("Class not defined")
    String className();
  }
}
