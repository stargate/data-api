package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerResponseContext;
import java.util.List;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds the {@code Billing-Events} HTTP response header (a JSON array of {@link BillingEvent}s
 * collected during the request) when {@link ApiFeature#BILLING_EVENTS_RESPONSE} is enabled.
 *
 * <p>If the feature is off, or no billing events were emitted, the header is not added. Failures to
 * serialize are logged and silently dropped so a serialization bug never breaks the actual API
 * response.
 */
@ApplicationScoped
public class BillingResponseFilter {

  /** HTTP response header that carries the JSON array of billing events. */
  public static final String BILLING_EVENTS_HEADER = "Billing-Events";

  private static final Logger LOGGER = LoggerFactory.getLogger(BillingResponseFilter.class);

  // ObjectWriter is thread-safe and expensive to build; share one across all requests.
  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

  private final RequestContext requestContext;

  @Inject
  public BillingResponseFilter(RequestContext requestContext) {
    this.requestContext = requestContext;
  }

  @ServerResponseFilter
  public void addBillingHeader(ContainerResponseContext responseContext) {
    if (!requestContext.apiFeatures().isFeatureEnabled(ApiFeature.BILLING_EVENTS_RESPONSE)) {
      return;
    }
    List<BillingEvent> events = requestContext.billing().collectedEvents();
    if (events.isEmpty()) {
      return;
    }
    try {
      responseContext
          .getHeaders()
          .add(BILLING_EVENTS_HEADER, OBJECT_WRITER.writeValueAsString(events));
    } catch (JsonProcessingException e) {
      LOGGER.error("Failed to serialize {} billing events to response header", events.size(), e);
    }
  }
}
