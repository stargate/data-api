package io.stargate.sgv2.jsonapi.config.feature;

import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import java.util.Collections;
import java.util.Map;

/**
 * Accessor for combined state of feature flags; typically based on static configuration (with its
 * overrides) and possible per-request settings. For code that wants to check whether given feature
 * is enabled or not, method to use is {@link ApiFeatures#isFeatureEnabled(ApiFeature)}. For details
 * on how configuration settings and request headers are combined, see {@link ApiFeature} and {@link
 * FeaturesConfig}
 */
public class ApiFeatures {
  private final Map<ApiFeature, Boolean> fromConfig;
  private final DataApiRequestInfo.HttpHeaderAccess httpHeaders;

  private ApiFeatures(
      Map<ApiFeature, Boolean> fromConfig, DataApiRequestInfo.HttpHeaderAccess httpHeaders) {
    this.fromConfig = fromConfig;
    this.httpHeaders = httpHeaders;
  }

  public static ApiFeatures empty() {
    return new ApiFeatures(Collections.emptyMap(), null);
  }

  public static ApiFeatures fromConfigAndRequest(
      FeaturesConfig config, DataApiRequestInfo.HttpHeaderAccess httpHeaders) {
    Map<ApiFeature, Boolean> fromConfig = config.flags();
    if (fromConfig == null) {
      fromConfig = Collections.emptyMap();
    }
    return new ApiFeatures(fromConfig, httpHeaders);
  }

  public boolean isFeatureEnabled(ApiFeature flag) {
    // First check if there is definition from configuration
    Boolean b = fromConfig.get(flag);

    if (b == null) {
      // and only if not, allow per-request specification
      if (httpHeaders != null) {
        b = httpHeaders.getHeaderAsBoolean(flag.httpHeaderName());
      }
    }
    return (b == null) ? flag.enabledByDefault() : b.booleanValue();
  }
}
