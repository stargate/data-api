package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.vertx.core.MultiMap;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for writing tests that use {@link io.stargate.sgv2.jsonapi.config.feature.ApiFeatures}
 */
public class ApiFeaturesTestUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiFeaturesTestUtil.class);

  /** See {@link #withFeature(ApiFeature, Boolean, Boolean)} */
  public static ApiFeatures withFeature(ApiFeature feature, Boolean configEnabled) {
    return ApiFeaturesTestUtil.withFeature(feature, configEnabled, null);
  }

  /**
   * Create a {@link ApiFeatures} that has either or both of the provided APIFeature specified by
   * config or the header.
   *
   * @param feature The {@link ApiFeature} we want to set value for
   * @param configEnabled If the feature should be set via config, if null is ignored.
   * @param headerEnabled If the feature should be set via HTTP Header, if null is ignored.
   * @return Configured {@link ApiFeatures}
   */
  public static ApiFeatures withFeature(
      ApiFeature feature, Boolean configEnabled, Boolean headerEnabled) {

    Map<ApiFeature, String> flags =
        configEnabled == null
            ? Collections.emptyMap()
            : Map.of(feature, String.valueOf(configEnabled));

    RequestContext.HttpHeaderAccess headers = null;
    if (headerEnabled != null) {
      var rawHeaders = MultiMap.caseInsensitiveMultiMap();
      rawHeaders.add(feature.httpHeaderName(), String.valueOf(headerEnabled));
      headers = new RequestContext.HttpHeaderAccess(rawHeaders);
    }

    LOGGER.debug("withFeature() - created APIFeatures. flags:{}, headers:{}", flags, headers);
    return ApiFeatures.fromConfigAndRequest(flags, headers);
  }
}
