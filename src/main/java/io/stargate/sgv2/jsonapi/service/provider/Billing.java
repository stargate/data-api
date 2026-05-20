package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits billing events as structured JSON log lines for downstream billing pipelines.
 *
 * <p>Each call to {@link #bill(ModelUsage, ApiFeatures)} for an eligible request emits two events:
 * a {@code *_tokens} event for token usage and a {@code *_egress_bytes} event for response bytes.
 * Eligibility requires all of: {@link ApiFeature#BILLING} is enabled, the {@code billing.events}
 * logger is enabled, and {@link BillingConfig#region()} is configured.
 */
@ApplicationScoped
public class Billing {

  private static final Logger BILLING_LOGGER = LoggerFactory.getLogger("billing.events");
  private static final Logger LOGGER = LoggerFactory.getLogger(Billing.class);

  private final BillingConfig config;
  private final ObjectWriter objectWriter;

  @Inject
  public Billing(BillingConfig config) {
    this.config = config;
    this.objectWriter = new ObjectMapper().writer();
  }

  /**
   * Emits billing events for the given aggregated model usage, if the request and configuration
   * allow it. No-op otherwise (feature disabled, logger disabled, null usage, or missing region
   * config).
   */
  public void bill(ModelUsage modelUsage, ApiFeatures apiFeatures) {
    if (!shouldEmit(modelUsage, apiFeatures)) {
      return;
    }
    String region = config.region().orElse(null);
    if (region == null) {
      LOGGER.error(
          "Billing event skipped: stargate.jsonapi.billing.region is not configured for tenant {}",
          modelUsage.tenant());
      return;
    }
    for (BillingEvent event : buildEvents(modelUsage, region)) {
      try {
        BILLING_LOGGER.info(objectWriter.writeValueAsString(event));
      } catch (JacksonException e) {
        LOGGER.error("Failed to serialize billing event of type {}", event.eventType(), e);
      }
    }
  }

  /**
   * Whether a billing event should be emitted for the given request. Package-private for testing.
   */
  boolean shouldEmit(ModelUsage modelUsage, ApiFeatures apiFeatures) {
    return apiFeatures.isFeatureEnabled(ApiFeature.BILLING)
        && BILLING_LOGGER.isInfoEnabled()
        && modelUsage != null;
  }

  /**
   * Builds the list of billing events for one {@link ModelUsage}: a {@code *_tokens} event with
   * {@link ModelUsage#totalTokens()} and an {@code *_egress_bytes} event with {@link
   * ModelUsage#responseBytes()}. Package-private for testing.
   *
   * <p>E.g. for an NVIDIA embeddings call with 7 tokens and 1024 response bytes, this returns two
   * events of the form:
   *
   * <pre>{@code
   * {"id":"...","timestamp":"...","product":"serverless",
   *  "event_type":"nvidia_embeddings_tokens",
   *  "properties":{"usage":7,"region":"us-west-2",
   *                "resource_type":"serverless_database","resource_id":"<tenant>"}}
   *
   * {"id":"...","timestamp":"...","product":"serverless",
   *  "event_type":"nvidia_egress_bytes",
   *  "properties":{"usage":1024,"region":"us-west-2",
   *                "resource_type":"serverless_database","resource_id":"<tenant>"}}
   * }</pre>
   */
  List<BillingEvent> buildEvents(ModelUsage modelUsage, String region) {
    String provider = modelUsage.modelProvider().apiName();
    String resourceId = modelUsage.tenant().toString();
    return List.of(
        newEvent(
            provider + "_" + modelUsage.modelType().billingName() + "_tokens",
            modelUsage.totalTokens(),
            region,
            resourceId),
        newEvent(provider + "_egress_bytes", modelUsage.responseBytes(), region, resourceId));
  }

  private BillingEvent newEvent(String eventType, long usage, String region, String resourceId) {
    var properties =
        new BillingEvent.BillingProperties(usage, region, config.resourceType(), resourceId);
    return new BillingEvent(
        UUID.randomUUID(), Instant.now(), config.product(), eventType, properties);
  }
}
