package io.stargate.sgv2.jsonapi.config.feature;

import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import java.util.Collections;
import java.util.Map;

/**
 * Accessor for combined state of feature flags; typically based on static configuration (with its
 * overrides) and possible per-request settings.
 */
public class ApiFeatures {
  private final Map<ApiFeature, Boolean> fromConfig;
  private final DataApiRequestInfo.HttpHeaderAccess httpHeaders;

  ApiFeatures(
      Map<ApiFeature, Boolean> fromConfig,
      DataApiRequestInfo.HttpHeaderAccess httpHeaders) {
    this.fromConfig = (fromConfig == null) ? Collections.emptyMap() : fromConfig;
    this.httpHeaders = httpHeaders;
  }

  public static ApiFeatures empty() {
    return new ApiFeatures(Collections.emptyMap(), null);
  }

  public static ApiFeatures fromConfigAndRequest(
          FeaturesConfig config, DataApiRequestInfo.HttpHeaderAccess httpHeaders) {
    return new ApiFeatures(config.flags(), httpHeaders);
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
