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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Adds tenantId and user agent to every metric */
@ApplicationScoped
public class TenantRequestMetricsTagProvider implements HttpServerMetricsTagsContributor {

  private final MetricsConfig.TenantRequestCounterConfig config;
  private final RequestContext requestContext;


  /** Default constructor. */
  @Inject
  public TenantRequestMetricsTagProvider(
      // NOTE: is RequestContext is request scoped.
      RequestContext requestContext,
      MetricsConfig metricsConfig) {

    this.requestContext = requestContext;
    this.config = metricsConfig.tenantRequestCounter();
  }

  @Override
  public Tags contribute(Context context) {

    List<Tag> tags = new ArrayList<>(2);
    tags.add(Tag.of(config.tenantTag(),requestContext.getTenant().toString()));

    if (config.userAgentTagEnabled()) {
      tags.add(Tag.of(config.userAgentTag(), requestContext.getUserAgent().product()));
    }

    return Tags.of(tags);
  }

}
