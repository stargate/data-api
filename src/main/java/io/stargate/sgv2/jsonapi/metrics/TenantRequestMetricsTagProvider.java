package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.regex.Pattern;

/** Tags provider for http request metrics. It provides tenant id and user agent as tags. */
@ApplicationScoped
public class TenantRequestMetricsTagProvider implements HttpServerMetricsTagsContributor {

  // split pattern for the user agent, extract only first part of the agent
  private static final Pattern USER_AGENT_SPLIT = Pattern.compile("[\\s/]");

  // same as V1 io.stargate.core.metrics.StargateMetricConstants#UNKNOWN
  private static final String UNKNOWN_VALUE = "unknown";

  /** The configuration for metrics. */
  private final MetricsConfig.TenantRequestCounterConfig config;

  /** The request info bean. */
  private final RequestContext requestContext;

  /** The tag for error being true, created only once. */
  private final Tag errorTrue;

  /** The tag for error being false, created only once. */
  private final Tag errorFalse;

  /** The tag for tenant being unknown, created only once. */
  private final Tag tenantUnknown;

  /** Default constructor. */
  @Inject
  public TenantRequestMetricsTagProvider(
      RequestContext requestContext, MetricsConfig metricsConfig) {
    this.requestContext = requestContext;
    this.config = metricsConfig.tenantRequestCounter();
    errorTrue = Tag.of(config.errorTag(), "true");
    errorFalse = Tag.of(config.errorTag(), "false");
    tenantUnknown = Tag.of(config.tenantTag(), UNKNOWN_VALUE);
  }

  @Override
  public Tags contribute(Context context) {
    // resolve tenant
    Tag tenantTag =
        requestContext
            .getTenantId()
            .map(id -> Tag.of(config.tenantTag(), id))
            .orElse(tenantUnknown);

    // check if we need user agent as well
    Tags tags = Tags.of(tenantTag);
    if (config.userAgentTagEnabled()) {
      String userAgentValue = getUserAgentValue(context.request());
      tags = tags.and(Tag.of(config.userAgentTag(), userAgentValue));
    }
    return tags;
  }

  private String getUserAgentValue(HttpServerRequest request) {
    String headerString = request.getHeader(HttpHeaders.USER_AGENT);
    if (null != headerString && !headerString.isBlank()) {
      String[] split = USER_AGENT_SPLIT.split(headerString);
      if (split.length > 0) {
        return split[0];
      } else {
        return headerString;
      }
    } else {
      return UNKNOWN_VALUE;
    }
  }
}
