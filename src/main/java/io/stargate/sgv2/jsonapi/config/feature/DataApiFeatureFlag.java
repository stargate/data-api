package io.stargate.sgv2.jsonapi.config.feature;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;

/**
 * Set of "Feature Flags" that can be used to enable/disable certain features in the Data API.
 * Enumeration defines the key used to introspect state of feature.
 */
public enum DataApiFeatureFlag {
  /**
   * API Tables feature flag: if enabled, the API will expose table-specific Namespace resource
   * commands, and support Collection commands on Tables. If disabled, those operations will fail
   * with {@link ErrorCode#TABLE_FEATURE_NOT_ENABLED}.
   *
   * <p>If no configuration specified (config or request), the feature will be Disabled.
   */
  TABLES("tables", false);

  private final String featureName;

  private final boolean enabledByDefault;

  /**
   * HTTP header name to be used to override the feature flag for a specific request: lower-case,
   * prefixed with "x-stargate-feature-"; lookup case-insensitive.
   */
  private final String featureNameAsHeader;

  DataApiFeatureFlag(String featureName, boolean enabledByDefault) {
    this.featureName = featureName;
    featureNameAsHeader = "x-stargate-feature-" + featureName;
    this.enabledByDefault = enabledByDefault;
  }

  public String featureName() {
    return featureName;
  }

  public String httpHeaderName() {
    return featureNameAsHeader;
  }

  public boolean enabledByDefault() {
    return enabledByDefault;
  }
}
