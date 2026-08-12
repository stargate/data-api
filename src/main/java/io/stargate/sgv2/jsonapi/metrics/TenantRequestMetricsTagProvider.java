package io.stargate.sgv2.jsonapi.metrics;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.UNKNOWN_VALUE;
import static io.stargate.sgv2.jsonapi.util.StringUtil.isNullOrBlank;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;

/** Tags provider for http request metrics. It provides tenant id and user agent as tags. */
@ApplicationScoped
public class TenantRequestMetricsTagProvider implements HttpServerMetricsTagsContributor {

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

  /**
   * The {@link UserAgent#product()} from the request context
   */
  private String getUserAgentValue(HttpServerRequest request) {

    String userAgent;
    try {
      userAgent = requestContext.userAgent().product();
    } catch (ContextNotActiveException | IllegalStateException e) {
      // no request context, see contribute() above. The full header is all we have, better than
      // reporting the agent as unknown.
      userAgent = request.getHeader(HttpHeaders.USER_AGENT);
    }
    return isNullOrBlank(userAgent) ? UNKNOWN_VALUE : userAgent;
  }
}
