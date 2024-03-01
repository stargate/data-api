package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.spi.Converter;

@ConfigMapping(prefix = "jsonapi.vector.provider")
public interface PropertyBasedEmbeddingServiceConfig {

  @Nullable
  VectorProviderConfig openai();

  @Nullable
  VectorProviderConfig huggingface();

  @Nullable
  VectorProviderConfig vertexai();

  @Nullable
  VectorProviderConfig cohere();

  @Nullable
  VectorProviderConfig nvidia();

  @Nullable
  CustomConfig custom();

  interface VectorProviderConfig {
    @JsonProperty
    boolean enabled();

    @JsonProperty
    String url();

    @JsonProperty
    String apiKey();

    @JsonProperty
    List<String> supportedAuthentication();

    @Nullable
    @JsonProperty
    List<ParameterConfig> parameters();

    @Nullable
    @JsonProperty
    Map<String, String> properties();

    @Nullable
    @JsonProperty
    List<ModelConfig> models();

    interface ModelConfig {
      @JsonProperty
      String name();

      @JsonProperty
      List<ParameterConfig> parameters();

      @JsonProperty
      Map<String, String> properties();
    }

    interface ParameterConfig {
      @JsonProperty
      String name();

      @JsonProperty
      ParameterType type();

      @JsonProperty
      boolean required();

      @Nullable
      @JsonProperty
      Optional<String> defaultValue();

      @Nullable
      @JsonProperty
      Optional<String> help();
    }

    enum ParameterType {
      STRING,
      NUMBER,
      BOOLEAN
    }
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
