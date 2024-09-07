package io.stargate.sgv2.jsonapi.config.feature;

import com.fasterxml.jackson.annotation.JsonValue;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;

/**
 * Set of "Feature Flags" that can be used to enable/disable certain features in the Data API.
 * Enumeration defines the key used to introspect state of feature.
 *
 * <p>NOTE: although flag names are in upper case (like {@code TABLES}), the actual configuration
 * uses lower-case names (like {@code tables}) (with proper prefix).
 *
 * <p>Usage: Features may be enabled via configuration: see {@link FeaturesConfig}; if defined at
 * that level, they are either enabled or disabled for all requests. If not defined (left as empty
 * or {@code null}}, HTTP Request headers can be used to enable/disable features on per-request
 * basis. Finally, if neither configuration nor request headers are used, {@link
 * ApiFeature#enabledByDefault()} value is used.
 */
public enum ApiFeature {
  /**
   * API Tables feature flag: if enabled, the API will expose table-specific Namespace resource
   * commands, and support Collection commands on Tables. If disabled, those operations will fail
   * with {@link ErrorCodeV1#TABLE_FEATURE_NOT_ENABLED}.
   *
   * <p>If no configuration specified (config or request), the feature will be Disabled.
   */
  TABLES("tables", false);

  /**
   * Prefix for HTTP headers used to override feature flags for specific requests: prepended before
   * {@link #featureName}, so f.ex for {@link #TABLES} flag, the header name would be {@code
   * Feature-Flag-tables}.
   */
  public static final String HTTP_HEADER_PREFIX = "Feature-Flag-";

  private final String featureName;

  private final boolean enabledByDefault;

  /**
   * HTTP header name to be used to override the feature flag for a specific request: lower-case,
   * prefixed with "x-stargate-feature-"; lookup case-insensitive.
   */
  private final String featureNameAsHeader;

  ApiFeature(String featureName, boolean enabledByDefault) {
    if (!featureName.equals(featureName.toLowerCase())) {
      throw new IllegalStateException(
          "Internal error: 'featureName' must be lower-case, was: \"" + featureName + "\"");
    }
    this.featureName = featureName;
    featureNameAsHeader = HTTP_HEADER_PREFIX + featureName;
    this.enabledByDefault = enabledByDefault;
  }

  @JsonValue // for Jackson to serialize as lower-case
  public String featureName() {
    return featureName;
  }

  public String httpHeaderName() {
    return featureNameAsHeader;
  }

  /**
   * Default state of the feature flag, if not explicitly configured (either by config or request).
   *
   * @return {@code true} if the feature is enabled by default, {@code false} otherwise.
   */
  public boolean enabledByDefault() {
    return enabledByDefault;
  }
}
