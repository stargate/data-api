package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Configuration mapping for API Tables feature. */
@ConfigMapping(prefix = "stargate.tables")
public interface ApiTablesConfig {
    @WithDefault("false")
    boolean enabled();
}
