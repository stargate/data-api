package io.stargate.sgv2.jsonapi.config.feature;

import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

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
    Objects.requireNonNull(config, "config cannot be null");
    return fromConfigAndRequest(config.flags(), httpHeaders);
  }

  public static ApiFeatures fromConfigAndRequest(
      Map<ApiFeature, String> flags, RequestContext.HttpHeaderAccess httpHeaders) {
    return new ApiFeatures(flags == null ? Collections.emptyMap() : flags, httpHeaders);
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
