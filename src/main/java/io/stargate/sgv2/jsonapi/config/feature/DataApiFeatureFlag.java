package io.stargate.sgv2.jsonapi.config.feature;

/**
 * Set of "Feature Flags" that can be used to enable/disable certain features in the Data API.
 * Enumeration defines the key used to introspect state of feature.
 */
public enum DataApiFeatureFlag {
  TABLES("tables", false);

  private final String featureName;

  private final boolean enabledByDefault;

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
