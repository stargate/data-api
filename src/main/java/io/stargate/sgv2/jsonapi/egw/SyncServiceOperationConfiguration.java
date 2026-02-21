package io.stargate.sgv2.jsonapi.egw;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/** Configuration for the operation execution of Sync Service */
@ConfigMapping(prefix = "stargate.sync-service")
public interface SyncServiceOperationConfiguration {

  /** Sync service retry configurations. */
  @NotNull
  @Valid
  SyncServiceOperationConfiguration.RetryConfig retry();

  interface RetryConfig {
    /**
     * The maximum number of retries to attempt before failing the request.
     *
     * @return The maximum number of retries to attempt before failing the request.
     */
    @Positive
    @WithDefault("3")
    int maxRetries();

    /**
     * The back off time between retries in milliseconds.
     *
     * @return The delay between retries in milliseconds.
     */
    @Positive
    @WithDefault("100")
    int retryDelayMillis();
  }
}
