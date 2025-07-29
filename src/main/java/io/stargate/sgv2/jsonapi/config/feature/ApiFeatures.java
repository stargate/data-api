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

  private ApiFeatures(
      Map<ApiFeature, String> fromConfig, RequestContext.HttpHeaderAccess httpHeaders) {
    this.fromConfig = fromConfig;
    this.httpHeaders = httpHeaders;
  }

  public static ApiFeatures empty() {
    return new ApiFeatures(Collections.emptyMap(), null);
  }

  public static ApiFeatures fromConfigAndRequest(
      FeaturesConfig config, RequestContext.HttpHeaderAccess httpHeaders) {
    Map<ApiFeature, String> fromConfig = config.flags();
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
    if (str == null || str.isBlank()) {
      return null; // no value, so not enabled
    }
    if ("true".equals(str)) {
      return Boolean.TRUE;
    }
    if ("false".equals(str)) {
      return Boolean.FALSE;
    }
    throw new IllegalArgumentException(
        "Invalid `Boolean` value: '" + str + "'. Expected 'true' or 'false'.");
  }
}
