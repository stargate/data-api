package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import jakarta.annotation.Nullable;
import java.net.URL;
import java.util.Optional;
import org.eclipse.microprofile.config.spi.Converter;

@ConfigMapping(prefix = "stargate.jsonapi.embedding.service")
public interface PropertyBasedEmbeddingServiceConfig {

  @Nullable
  OpenaiConfig openai();

  @Nullable
  HuggingFaceConfig huggingface();

  @Nullable
  VertexAiConfig vertexai();

  @Nullable
  CustomConfig custom();

  public interface OpenaiConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://api.openai.com/v1")
    URL url();

    @WithDefault("Bearer")
    String apiKey();
  }

  public interface HuggingFaceConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://api-inference.huggingface.co")
    URL url();

    @WithDefault("")
    String apiKey();
  }

  public interface VertexAiConfig {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("https://us-central1-aiplatform.googleapis.com/v1")
    URL url();

    @WithDefault("")
    String apiKey();
  }

  public interface CustomConfig {
    @WithDefault("false")
    boolean enabled();

    @Nullable
    @WithConverter(ClassNameResolver.class)
    Optional<Class<?>> clazz();
  }

  class ClassNameResolver implements Converter<Class<?>> {
    @Override
    public Class<?> convert(String value) {
      if (value != null && !value.isEmpty())
        try {
          return Class.forName(value);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      return null;
    }
  }
}
