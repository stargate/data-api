package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import jakarta.annotation.Nullable;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.spi.Converter;

@ConfigMapping(prefix = "stargate.jsonapi.embedding.service")
public interface PropertyBasedEmbeddingServiceConfig {

  @Nullable
  ServiceConfig openai();

  @Nullable
  ServiceConfig huggingface();

  @Nullable
  ServiceConfig vertexai();

  @Nullable
  ServiceConfig cohere();

  @Nullable
  ServiceConfig nvidia();

  @Nullable
  VectorProviderConfig google();

  @Nullable
  CustomConfig custom();

  public interface ServiceConfig {
    boolean enabled();

    URL url();

    String apiKey();
  }

  interface VectorProviderConfig {
    @JsonProperty
    boolean enabled();

    @JsonProperty
    String url();

    @JsonProperty
    List<String> supportedAuthentication();

    @JsonProperty
    List<ParameterConfig> parameters();

    @JsonProperty
    Map<String, String> properties();

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

      @JsonProperty
      Optional<String> defaultValue();

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
