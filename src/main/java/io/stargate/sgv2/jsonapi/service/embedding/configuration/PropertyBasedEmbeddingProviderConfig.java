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

@ConfigMapping(prefix = "stargate.jsonapi.embedding")
public interface PropertyBasedEmbeddingProviderConfig {

  @Nullable
  Map<String, EmbeddingProviderConfig> providers();

  @Nullable
  CustomConfig custom();

  interface EmbeddingProviderConfig {
    @JsonProperty
    boolean enabled();

    @JsonProperty
    String url();

    @JsonProperty
    String apiKey();

    @JsonProperty
    List<String> supportedAuthentication();

    /**
     * A list of parameters for user customization. Parameters are used to construct the URL or to
     * customize the model according to user requirements.
     *
     * @return A list of parameters representing the customizable aspects of the model for external
     *     users, or empty list if no parameters are defined.
     */
    @Nullable
    @JsonProperty
    List<ParameterConfig> parameters();

    /**
     * A map of internal properties used for model interaction. Properties are utilized by the
     * system to configure the actual model usage, such as retry policies and timeouts.
     *
     * @return A map of internal configuration properties, or empty map if none are defined.
     */
    @JsonProperty
    RequestProperties properties();

    @Nullable
    @JsonProperty
    List<ModelConfig> models();

    interface ModelConfig {
      @JsonProperty
      String name();

      @JsonProperty
      Integer vectorDimension();

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

    /** A set of http properties used for request to the embedding providers. */
    interface RequestProperties {

      /**
       * The maximum number of retries to attempt before failing the request.
       *
       * @return The maximum number of retries to attempt before failing the request.
       */
      @WithDefault("3")
      int maxRetries();

      /**
       * The back off time between retries in milliseconds.
       *
       * @return The delay between retries in milliseconds.
       */
      @WithDefault("100")
      int retryDelayMillis();

      /**
       * The timeout for the request in milliseconds.
       *
       * @return The timeout for the request in milliseconds.
       */
      @WithDefault("10000")
      int requestTimeoutMillis();
    }

    enum ParameterType {
      STRING,
      NUMBER,
      BOOLEAN
    }
  }

  interface CustomConfig {
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
