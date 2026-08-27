package io.stargate.sgv2.jsonapi.config.feature;

import io.smallrye.config.ConfigMapping;
import java.util.Map;

/**
 * Configuration mapping for Data API Feature flags as read from main application configuration
 * (with possible property / sysenv overrides).
 *
 * <p>Feature flags can be configured using either property file format (with dots) or environment
 * variables (with underscores). The configuration uses the prefix {@code stargate.feature.flags}
 * followed by the feature name from {@link ApiFeature#featureName()} (always in lower-case
 * kebab-case format).
 *
 * <p><b>Property File Examples (application.yaml, application.properties):</b>
 *
 * <pre>
 * stargate.feature.flags.lexical=true
 * stargate.feature.flags.tables=false
 * stargate.feature.flags.reranking=true
 * stargate.feature.flags.mcp=false
 * stargate.feature.flags.request-tracing=true
 * stargate.feature.flags.billing-events-logging=true
 * </pre>
 *
 * <p><b>Environment Variable Examples:</b>
 *
 * <pre>
 * export STARGATE_FEATURE_FLAGS_LEXICAL=true
 * export STARGATE_FEATURE_FLAGS_TABLES=false
 * export STARGATE_FEATURE_FLAGS_RERANKING=true
 * export STARGATE_FEATURE_FLAGS_MCP=false
 * export STARGATE_FEATURE_FLAGS_REQUEST_TRACING=true
 * export STARGATE_FEATURE_FLAGS_BILLING_EVENTS_LOGGING=true
 * </pre>
 *
 * <p><b>Note:</b> Quarkus/SmallRye Config automatically translates underscores ({@code _}) in
 * environment variable names to dots ({@code .}) when matching configuration keys. Feature flag
 * values are bound as Strings and converted to Boolean when needed. Null values are not accepted.
 *
 * @see ApiFeature for available feature flags
 * @see ApiFeatures for runtime feature flag evaluation
 */
@ConfigMapping(prefix = "stargate.feature")
public interface FeaturesConfig {

  /**
   * Returns a map of feature flag names to their configured values. Keys are feature names in
   * kebab-case format (e.g., "lexical", "billing-events-logging"). Values are string
   * representations of boolean flags ("true", "false", or blank for undefined).
   *
   * @return map of feature flag configurations, never null but may be empty
   */
  Map<String, String> flags();
}
