/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.HttpHeaders;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;

/**
 * The filter for counting HTTP requests per tenant. Controlled by {@link
 * MetricsConfig.TenantRequestCounterConfig}.
 * <p>
 * Changes:
 * <ul>
 *   <li>previously the tenant tag would be "unknown" if it was not know, see Tenant class now. </li>
 * </ul>
 */
@ApplicationScoped
public class TenantRequestMetricsFilter {

  // split pattern for the user agent, extract only first part of the agent
  private static final Pattern USER_AGENT_SPLIT = Pattern.compile("[\\s/]");


  private final MeterRegistry meterRegistry;
  private final MetricsConfig.TenantRequestCounterConfig config;
  // using different name to avoid name collision in record()
  private final RequestContext apiRequestContext;

  private final Tag errorTrueTag;
  private final Tag errorFalseTag;


  /** Default constructor. */
  @Inject
  public TenantRequestMetricsFilter(
      MeterRegistry meterRegistry,
      // NOTE: RequestContext is request scoped.
      RequestContext apiRequestContext,
      MetricsConfig metricsConfig) {

    this.meterRegistry = meterRegistry;
    this.apiRequestContext = apiRequestContext;
    this.config = metricsConfig.tenantRequestCounter();

    this.errorTrueTag = Tag.of(config.errorTag(), "true");
    this.errorFalseTag = Tag.of(config.errorTag(), "false");
  }

  /**
   * Filter that this bean produces.
   * <p>
   *   see https://quarkus.io/guides/resteasy-reactive#request-or-response-filters
   * </p>
   * @param requestContext {@link ContainerRequestContext}
   * @param responseContext {@link ContainerResponseContext}
   *
   */
  @ServerResponseFilter
  public void record(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {

    if (!config.enabled()) {
      return;
    }

    List<Tag> tags = new ArrayList<>(4);
    tags.add(Tag.of(config.tenantTag(), apiRequestContext.getTenant().toString()));
    tags.add(responseContext.getStatus() >= 500 ? errorTrueTag : errorFalseTag);

      if (config.userAgentTagEnabled()) {
        tags.add(Tag.of(config.userAgentTag(), apiRequestContext.getUserAgent().product()));
      }
      if (config.statusTagEnabled()) {
        tags.add(Tag.of(config.statusTag(), String.valueOf(responseContext.getStatus())));
      }
      meterRegistry.counter(config.metricName(), tags).increment();
  }
}
