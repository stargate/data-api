package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.spi.Converter;

public interface EmbeddingProvidersConfig {
  Map<String, EmbeddingProviderConfig> providers();

  @Nullable
  @Inject
  CustomConfig custom();

  interface EmbeddingProviderConfig {
    @JsonProperty
    String displayName();

    @JsonProperty
    boolean enabled();

    @JsonProperty
    String url();

    /**
     * A map of supported authentications. HEADER, SHARED_SECRET and NONE are the only techniques
     * the DataAPI supports (i.e. the key of map can only be HEADER, SHARED_SECRET or NONE).
     *
     * @return
     */
    @JsonProperty
    Map<AuthenticationType, AuthenticationConfig> supportedAuthentications();

    enum AuthenticationType {
      NONE,
      HEADER,
      SHARED_SECRET
    }

    /**
     * enabled() is a JSON boolean to flag if this technique is supported. If false the rest of the
     * object has no impact. Any technique not listed is also not supported for the provider.
     *
     * <p>tokens() is a list of token mappings, that map from the name accepted by the Data API to
     * how they are forwarded to the provider. The provider information is included for the code,
     * and to allow users to see what we do with the values.
     */
    interface AuthenticationConfig {
      @JsonProperty
      boolean enabled();

      @JsonProperty
      List<TokenConfig> tokens();
    }

    /**
     * For the HEADER technique the `accepted` value is the name of the header the client should
     * send, and `forwarded` is the name of the header the Data API will send to the provider.
     *
     * <p>For the SHARED_SECRET technique the `accepted` value is the name used in the
     * authentication option with createCollection that maps to the name of a shared secret, and
     * `forwarded` is the name of the header the Data API will send to the provider.
     */
    interface TokenConfig {
      @JsonProperty
      String accepted();

      @JsonProperty
      String forwarded();
    }

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

      /**
       * vectorDimension is not null if the model supports a single dimension value. It will be null
       * if the model supports different dimensions. A parameter called vectorDimension is included.
       *
       * @return
       */
      @Nullable
      @JsonProperty
      Optional<Integer> vectorDimension();

      @Nullable
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

      /**
       * validation is an object that describes how the Data API will validate the parameters, and
       * how the UI may want to provide data entry hints. Only one of the validation methods will be
       * specified for each parameter.
       *
       * <p>`numericRange` if present is an array of two numbers that represent the inclusive value
       * range for a number parameter. E.g. the dimensions for the text-embedding-3
       *
       * <p>`options` if present is an array of valid options the user must select from, for example
       * if a model supports 3 different dimensions. If options are present the only allowed values
       * for the parameter are those in the options list. If not present, null, or an empty array
       * any value of the correct type is accepted.
       *
       * @return
       */
      @Nullable
      @JsonProperty
      Map<ValidationType, List<Integer>> validation();

      @Nullable
      @JsonProperty
      Optional<String> help();
    }

    enum ValidationType {
      NUMERIC_RANGE("numericRange"),
      OPTIONS("options");

      private final String type;

      ValidationType(final String type) {
        this.type = type;
      }

      public static ValidationType fromString(String type) {
        if (type.equals("numericRange")) {
          return NUMERIC_RANGE;
        } else if (type.equals("options")) {
          return OPTIONS;
        }
        throw ErrorCode.INVALID_PARAMETER_VALIDATION_TYPE.toApiException(type);
      }

      @Override
      public String toString() {
        return type;
      }
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

      @Nullable
      Optional<String> maxInputLength();

      @Nullable
      Optional<String> taskTypeStore();

      @Nullable
      Optional<String> taskTypeRead();

      /** Maximum batch size supported by the provider. */
      int maxBatchSize();
    }

    enum ParameterType {
      STRING("string"),
      NUMBER("number"),
      BOOLEAN("boolean");

      private final String type;

      ParameterType(final String type) {
        this.type = type;
      }

      @Override
      public String toString() {
        return type;
      }
    }
  }

  @ConfigMapping(prefix = "stargate.jsonapi.custom.embedding")
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
