package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Configuration for the billing event pipeline. */
@ConfigMapping(prefix = "stargate.jsonapi.billing")
public interface BillingConfig {

  @WithDefault("serverless")
  String product();

  @WithDefault("serverless_database")
  String resourceType();
}
