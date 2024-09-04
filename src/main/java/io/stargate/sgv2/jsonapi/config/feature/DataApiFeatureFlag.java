package io.stargate.sgv2.jsonapi.config.feature;

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
