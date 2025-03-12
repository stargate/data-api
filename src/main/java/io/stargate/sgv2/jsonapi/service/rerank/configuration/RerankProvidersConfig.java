package io.stargate.sgv2.jsonapi.service.rerank.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.WithDefault;
import java.util.List;
import java.util.Map;

public interface RerankProvidersConfig {
  Map<String, RerankProviderConfig> providers();

  interface RerankProviderConfig {
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

      @JsonProperty
      @WithDefault("false")
      boolean isDefault();

      @JsonProperty
      String url();

      @JsonProperty
      RequestProperties properties();

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
