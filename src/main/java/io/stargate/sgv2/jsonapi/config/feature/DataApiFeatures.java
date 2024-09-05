package io.stargate.sgv2.jsonapi.config.feature;

import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import java.util.Collections;
import java.util.Map;

/**
 * Accessor for combined state of feature flags; typically based on static configuration (with its
 * overrides) and possible per-request settings.
 */
public class DataApiFeatures {
  private final Map<DataApiFeatureFlag, Boolean> fromConfig;
  private final DataApiRequestInfo.HttpHeaderAccess httpHeaders;

  DataApiFeatures(
      Map<DataApiFeatureFlag, Boolean> fromConfig,
      DataApiRequestInfo.HttpHeaderAccess httpHeaders) {
    this.fromConfig = (fromConfig == null) ? Collections.emptyMap() : fromConfig;
    this.httpHeaders = httpHeaders;
  }

  public static DataApiFeatures empty() {
    return new DataApiFeatures(Collections.emptyMap(), null);
  }

  public static DataApiFeatures fromConfigAndRequest(
      DataApiFeatureConfig config, DataApiRequestInfo.HttpHeaderAccess httpHeaders) {
    return new DataApiFeatures(config.flags(), httpHeaders);
  }

  public boolean isFeatureEnabled(DataApiFeatureFlag flag) {
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
