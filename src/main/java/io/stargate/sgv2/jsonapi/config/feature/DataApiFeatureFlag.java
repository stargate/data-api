package io.stargate.sgv2.jsonapi.config.feature;

/**
 * Set of "Feature Flags" that can be used to enable/disable certain features in the Data API.
 * Enumeration defines the key used to introspect state of feature.
 */
public enum DataApiFeatureFlag {
  TABLES("tables");

  private final String featureName;

  DataApiFeatureFlag(String featureName) {
    this.featureName = featureName;
  }

  public String featureName() {
    return featureName;
  }
}
