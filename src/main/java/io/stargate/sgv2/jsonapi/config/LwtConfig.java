package io.stargate.sgv2.jsonapi.config;

import javax.validation.constraints.Max;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration setup for the Light-weigh transactions.
 */
@ConfigMapping(prefix = "stargate.jsonapi.lwt")
public interface LwtConfig {

    /** @return Defines the maximum retry for lwt failure <code>3</code>. */
    @Max(5)
    @PositiveOrZero
    @WithDefault("3")
    int retries();
}
