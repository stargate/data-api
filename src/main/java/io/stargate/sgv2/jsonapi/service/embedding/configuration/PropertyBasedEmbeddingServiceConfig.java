package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
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
    Map<String, ParameterConfig> parameters();

    @JsonProperty
    PropertiesConfig properties();

    @JsonProperty
    Map<String, ModelConfig> models();

    interface ParameterConfig {
      @JsonProperty
      String type();

      @JsonProperty
      boolean required();

      @JsonProperty
      @WithName("default")
      Optional<String> default_();
    }

    interface PropertiesConfig {
      @JsonProperty
      Optional<Integer> maxRetries();

      @JsonProperty
      Optional<Integer> retryDelayMs();

      @JsonProperty
      Optional<Integer> timeoutMs();

      @JsonProperty
      Optional<Integer> maxTokens();
    }

    interface ModelConfig {
      @JsonProperty
      Map<String, ParameterConfig> parameters();

      @JsonProperty
      PropertiesConfig properties();
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
