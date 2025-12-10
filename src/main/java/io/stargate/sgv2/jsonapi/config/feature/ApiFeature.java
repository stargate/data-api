package io.stargate.sgv2.jsonapi.config.feature;

import com.fasterxml.jackson.annotation.JsonValue;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;

/**
 * Set of "Feature Flags" that can be used to enable/disable certain features in the Data API.
 * Enumeration defines the key used to introspect state of feature.
 *
 * <p>NOTE: although flag names are in upper case (like {@code LEXICAL}), the actual configuration
 * uses lower-case names (like {@code lexical}) (with proper prefix).
 *
 * <p>Usage: Features may be enabled via configuration: see {@link FeaturesConfig}; if defined at
 * that level, they are either enabled or disabled for all requests. If not defined (left as empty
 * or {@code null}), HTTP Request headers can be used to enable/disable features on per-request
 * basis. Finally, if neither configuration nor request headers are used, feature is disabled.
 */
public enum ApiFeature {
  /**
   * Lexical search/sort feature flag: if enabled, the API will allow construction of
   * "$lexical"-enabled Collections. If disabled, those operations will fail with {@link
   * io.stargate.sgv2.jsonapi.exception.SchemaException.Code#LEXICAL_NOT_AVAILABLE_FOR_DATABASE}).
   *
   * <p>Enabled by default.
   */
  LEXICAL("lexical", true),

  /**
   * API Tables feature flag: if enabled, the API will expose table-specific Namespace resource
   * commands, and support commands on Tables. Deprecated -- no longer used.
   *
   * @deprecated since 1.0.35 -- but exists in Helm charts; remove once those removed
   */
  @Deprecated // since 1.0.35
  TABLES("tables", true),

  /**
   * API Reranking feature flag: if enabled, the API will expose:
   *
   * <ul>
   *   <li>CreateCollection and CreateTable commands with reranking config.
   *   <li>FindRerankingProviders command.
   *   <li>FindAndRerank command.
   * </ul>
   *
   * If disabled, those operations will fail with {@link ErrorCodeV1#RERANKING_FEATURE_NOT_ENABLED}.
   *
   * <p>Disabled by default.
   */
  RERANKING("reranking", false),

  /**
   * The request will return a trace of the processing that includes a message of the steps taken,
   * but excludes the data of the message which can be large.
   */
  REQUEST_TRACING("request-tracing", false),

  /**
   * The request will return a trace of the processing that includes both the message and the data.
   */
  REQUEST_TRACING_FULL("request-tracing-full", false);

  /**
   * Prefix for HTTP headers used to override feature flags for specific requests: prepended before
   * {@link #featureName()}, so f.ex for {@link #LEXICAL} flag, the header name would be {@code
   * Feature-Flag-lexical}.
   */
  public static final String HTTP_HEADER_PREFIX = "Feature-Flag-";

  /**
   * Feature flag name, in lower-case with hyphens (aka "kebab case"), used for as serialization
   * (JSON and YAML config files) as well as for constructing HTTP header names.
   */
  private final String featureName;

  /**
   * HTTP header name to be used to override the feature flag for a specific request: lower-case,
   * prefixed with "x-stargate-feature-"; lookup case-insensitive.
   */
  private final String featureNameAsHeader;

  /**
   * State of feature if not otherwise specified: if {@code true}, feature is enabled by default;
   * otherwise disabled.
   */
  private final boolean enabledByDefault;

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

  public boolean enabledByDefault() {
    return enabledByDefault;
  }
}
