package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * Configuration for the billing event pipeline.
 *
 * <p>{@code region} is required for billing to actually emit events; if it is not configured the
 * {@link io.stargate.sgv2.jsonapi.service.provider.Billing} component logs an error and skips the
 * event rather than emitting one with a missing region.
 */
@ConfigMapping(prefix = "stargate.jsonapi.billing")
public interface BillingConfig {

  @WithDefault("serverless")
  String product();

  @WithDefault("serverless_database")
  String resourceType();

  Optional<String> region();
}
