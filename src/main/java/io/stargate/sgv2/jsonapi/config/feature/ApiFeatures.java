package io.stargate.sgv2.jsonapi.config.feature;

import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import java.util.Collections;
import java.util.Map;

/**
 * Accessor for combined state of feature flags; typically based on static configuration (with its
 * overrides) and possible per-request settings. For code that wants to check whether given feature
 * is enabled or not, method to use is {@link ApiFeatures#isFeatureEnabled(ApiFeature)}. For details
 * on how configuration settings and request headers are combined, see {@link ApiFeature} and {@link
 * FeaturesConfig}
 *
 * <p>To get the features for the request use {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandContext#apiFeatures()}
 */
public class ApiFeatures {
  private final Map<ApiFeature, String> fromConfig;
  private final RequestContext.HttpHeaderAccess httpHeaders;

  private ApiFeatures(Map<String, String> fromConfig, RequestContext.HttpHeaderAccess httpHeaders) {
    this.fromConfig = marshallFromConfig(fromConfig);
    this.httpHeaders = httpHeaders;
  }

  public static ApiFeatures empty() {
    return new ApiFeatures(Collections.emptyMap(), null);
  }

  protected Map<ApiFeature, String> marshallFromConfig(Map<String, String> fromConfig) {
    if (fromConfig == null || fromConfig.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<ApiFeature, String> result = new java.util.HashMap<>();
    for (Map.Entry<String, String> entry : fromConfig.entrySet()) {
      String key = entry.getKey();
      ApiFeature feature = findFeatureByName(key);
      if (feature == null) {
        throw new IllegalArgumentException(
            "Invalid feature flag key: '"
                + key
                + "'. Expected one of: "
                + java.util.Arrays.stream(ApiFeature.values())
                    .map(
                        f ->
                            "STARGATE_FEATURE_FLAGS_"
                                + f.featureName().toUpperCase().replace('-', '_'))
                    .collect(java.util.stream.Collectors.joining(", ")));
      }
      result.put(feature, entry.getValue());
    }
    return result;
  }

  private ApiFeature findFeatureByName(String name) {
    for (ApiFeature feature : ApiFeature.values()) {
      if (feature.featureName().equals(name)) {
        return feature;
      }
    }
    return null;
  }

  public static ApiFeatures fromConfigAndRequest(
      FeaturesConfig config, RequestContext.HttpHeaderAccess httpHeaders) {
    Map<String, String> fromConfig = config.flags();
    if (fromConfig == null) {
      fromConfig = Collections.emptyMap();
    }
    return new ApiFeatures(fromConfig, httpHeaders);
  }

  public boolean isFeatureEnabled(ApiFeature flag) {
    // First check if there is definition from configuration
    Boolean b = booleanFromString(fromConfig.get(flag));
    if (b == null) {
      // and only if not, allow per-request specification
      if (httpHeaders != null) {
        b = httpHeaders.getHeaderAsBoolean(flag.httpHeaderName());
      }
    }
    if (b != null) {
      return b.booleanValue();
    }
    return flag.enabledByDefault();
  }

  private Boolean booleanFromString(String str) {
    // We will allow "*" as an alias in case config file cannot contain blank String value
    if (str == null || str.isBlank() || "*".equals(str)) {
      return null; // undefined
    }
    if ("true".equals(str)) {
      return Boolean.TRUE;
    }
    if ("false".equals(str)) {
      return Boolean.FALSE;
    }
    throw new IllegalArgumentException(
        "Invalid `Boolean` value: '"
            + str
            + "'. Expected 'true', 'false' or blank String (undefined).");
  }
}
