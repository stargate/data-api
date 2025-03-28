package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.WithDefault;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RerankingProvidersConfig {
  Map<String, RerankingProviderConfig> providers();

  interface RerankingProviderConfig {
    @JsonProperty
    @WithDefault("false")
    boolean isDefault();

    @JsonProperty
    String displayName();

    @JsonProperty
    boolean enabled();

    @JsonProperty
    Map<AuthenticationType, AuthenticationConfig> supportedAuthentications();

    enum AuthenticationType {
      NONE,
      HEADER,
      SHARED_SECRET
    }

    interface AuthenticationConfig {
      @JsonProperty
      boolean enabled();

      @JsonProperty
      List<TokenConfig> tokens();
    }

    interface TokenConfig {
      @JsonProperty
      String accepted();

      @JsonProperty
      String forwarded();
    }

    //    @Nullable
    @JsonProperty
    List<ModelConfig> models();

    interface ModelConfig {
      @JsonProperty
      String name();

      /**
       * modelSupport marks the support status of the model and optional message for the
       * deprecation, EOL etc.
       */
      @JsonProperty
      ModelSupport modelSupport();

      @JsonProperty
      @WithDefault("false")
      boolean isDefault();

      @JsonProperty
      String url();

      @JsonProperty
      RequestProperties properties();

      /**
       * By default, model is supporting and has no message. So if model-support is not configured
       * in the config source, it will be supporting by default.
       *
       * <p>If the model is deprecated or EOF, it will be marked in the config source and been
       * mapped.
       *
       * <p>If message is not configured in config source, it will be Optional.empty().
       */
      interface ModelSupport {
        @JsonProperty
        @WithDefault("SUPPORTING")
        SupportStatus status();

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Optional<String> message();

        enum SupportStatus {
          SUPPORTING("SUPPORTING"),
          DEPRECATED("DEPRECATED"),
          END_OF_LIFE("END_OF_LIFE");

          public final String status;

          SupportStatus(String status) {
            this.status = status;
          }
        }
      }

      interface RequestProperties {
        /**
         * Specifies the maximum number of attempts before failing. Default is 3 (1 request + 2
         * retries).
         *
         * @return The maximum number of attempts before failing.
         */
        @WithDefault("3")
        int atMostRetries();

        /**
         * The initial delay between retries in milliseconds. The first retry occurs after the
         * specified delay (default 100 ms), doubling each time until reaching maxBackOffMillis.
         *
         * @return The initial delay between retries in milliseconds.
         */
        @WithDefault("100")
        int initialBackOffMillis();

        /**
         * The maximum duration for a request to complete in milliseconds. Default is 5000 ms (5
         * seconds), ensuring retries are likely within general gateway timeouts.
         *
         * @return The request timeout in milliseconds.
         */
        @WithDefault("5000")
        int readTimeoutMillis();

        /**
         * The maximum delay between retries in milliseconds.
         *
         * @return The maximum delay between retries in milliseconds.
         */
        @WithDefault("500")
        int maxBackOffMillis();

        /**
         * A random variation added to the delay between retries in an exponential backoff strategy.
         * For example, with a base delay of 1 second and jitter of 0.2, the actual delay ranges
         * between 0.8 and 1.2 seconds.
         *
         * @return The jitter factor for backoff time.
         */
        @WithDefault("0.5")
        double jitter();

        /** Maximum batch size supported by the provider. */
        int maxBatchSize();
      }
    }
  }
}
