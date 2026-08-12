package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.UNKNOWN_VALUE;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.regex.Pattern;

/** Tags provider for http request metrics. It provides tenant id and user agent as tags. */
@ApplicationScoped
public class TenantRequestMetricsTagProvider implements HttpServerMetricsTagsContributor {

  // split pattern for the user agent, extract only first part of the agent
  private static final Pattern USER_AGENT_SPLIT = Pattern.compile("[\\s/]");

  /** The configuration for metrics. */
  private final MetricsConfig.TenantRequestCounterConfig config;

  /** The request info bean. */
  private final RequestContext requestContext;

  /** Default constructor. */
  @Inject
  public TenantRequestMetricsTagProvider(
      RequestContext requestContext, MetricsConfig metricsConfig) {
    this.requestContext = requestContext;
    this.config = metricsConfig.tenantRequestCounter();
  }

  /**
   * Prometheus requires all meters with the same name to have the same tag keys. If this
   * contributor throws (e.g. no active request scope for early-rejected requests), Quarkus records
   * the HTTP metric without our tags, poisoning the meter's key set — so always resolve to a
   * fallback value instead of failing.
   *
   * <p>This can happen when there is a call for an unmapped route, e.g. get call to `/unknown`, and
   * injected `requestContext` is not available.
   */
  @Override
  public Tags contribute(Context context) {

    String tenantValue;
    try {
      tenantValue = requestContext.tenant().toString();
    } catch (ContextNotActiveException | IllegalStateException e) {
      // no request context, this is only going to happen in rare situations
      // ContextNotActiveException: request never entered the request scope, e.g. rejected before
      // reaching JAX-RS on an unmatched route like GET /unknown. See PR#2538
      // IllegalStateException ("No REST request in progress"): MCP requests — routed through
      // Vert.x, not JAX-RS, so creating the RequestContext bean here fails when its constructor
      // asks RESTEasy for the SecurityContext.
      tenantValue = UNKNOWN_VALUE;
    }
    Tag tenantTag = Tag.of(config.tenantTag(), tenantValue);

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
