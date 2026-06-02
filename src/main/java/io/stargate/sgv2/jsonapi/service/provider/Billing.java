package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits billing events as structured JSON log lines for downstream billing pipelines.
 *
 * <p>One instance is created per request and lives on the {@link
 * io.stargate.sgv2.jsonapi.api.request.RequestContext} — get it via {@code
 * requestContext.billing()}. {@link ApiFeatures} is captured at construction time so callers only
 * need to pass the {@link ModelUsage}.
 *
 * <p>For each eligible {@link ModelUsage}, up to three events are emitted, one per billable
 * dimension ({@link BillingEventType.Dimension#TOTAL_TOKENS TOTAL_TOKENS}, {@link
 * BillingEventType.Dimension#EGRESS_BYTES EGRESS_BYTES}, {@link
 * BillingEventType.Dimension#INGRESS_BYTES INGRESS_BYTES}). The {@code internal_*} variant is used
 * when the model provider is listed in {@link BillingConfig#internalModelProviders()}; otherwise
 * the {@code external_*} variant is used. Events whose type is not in {@link
 * BillingConfig#enabledEventTypes()} are dropped.
 *
 * <p>Eligibility requires all of: {@link ApiFeature#BILLING} is enabled, the {@code billing.events}
 * logger is enabled, and the {@link ModelUsage} is non-null. The region for each event is read from
 * {@link ModelUsage#tenant()} ({@code Tenant.region()}).
 */
public class Billing {

  private static final Logger BILLING_LOGGER = LoggerFactory.getLogger("billing.events");
  private static final Logger LOGGER = LoggerFactory.getLogger(Billing.class);

  // ObjectMapper / ObjectWriter construction is expensive (serializers are built and cached on
  // first use); share a single thread-safe writer across all per-request Billing instances.
  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

  private final String product;
  private final String resourceType;
  private final Set<String> internalModelProviders;
  private final Set<BillingEventType> enabledEventTypes;
  private final ApiFeatures apiFeatures;

  public Billing(BillingConfig config, ApiFeatures apiFeatures) {
    Objects.requireNonNull(config, "config must not be null");
    this.apiFeatures = Objects.requireNonNull(apiFeatures, "apiFeatures must not be null");
    this.product = requireNonBlank(config.product(), "billing.product");
    this.resourceType = requireNonBlank(config.resourceType(), "billing.resource_type");
    this.internalModelProviders = Set.copyOf(config.internalModelProviders());
    this.enabledEventTypes = Set.copyOf(config.enabledEventTypes());
  }

  /**
   * Emits billing events for the given aggregated model usage, if the request and configuration
   * allow it. No-op otherwise (feature disabled, logger disabled, null usage).
   */
  public void bill(ModelUsage modelUsage) {
    if (!shouldEmit(modelUsage)) {
      return;
    }
    for (BillingEvent event : buildEvents(modelUsage)) {
      try {
        BILLING_LOGGER.info(OBJECT_WRITER.writeValueAsString(event));
      } catch (JacksonException e) {
        LOGGER.error("Failed to serialize billing event of type {}", event.eventType(), e);
      }
    }
  }

  /** Whether a billing event should be emitted for the given request. */
  @VisibleForTesting
  boolean shouldEmit(ModelUsage modelUsage) {
    return apiFeatures.isFeatureEnabled(ApiFeature.BILLING)
        && BILLING_LOGGER.isInfoEnabled()
        && modelUsage != null;
  }

  /**
   * Builds the list of billing events for one {@link ModelUsage}: one event per billable dimension
   * (total tokens, egress bytes, ingress bytes), with the {@code internal_*} or {@code external_*}
   * variant chosen based on {@link BillingConfig#internalModelProviders()}. Events whose type is
   * not in {@link BillingConfig#enabledEventTypes()} are filtered out.
   */
  @VisibleForTesting
  List<BillingEvent> buildEvents(ModelUsage modelUsage) {
    boolean internal = internalModelProviders.contains(modelUsage.modelProvider().apiName());
    String region = modelUsage.tenant().region();
    String resourceId = modelUsage.tenant().toString();
    String providerName = modelUsage.modelProvider().apiName();
    String modelName = modelUsage.modelName();

    var events = new ArrayList<BillingEvent>(3);
    addEventIfEnabled(
        events,
        BillingEventType.of(BillingEventType.Dimension.TOTAL_TOKENS, internal),
        modelUsage.totalTokens(),
        region,
        resourceId,
        providerName,
        modelName);
    addEventIfEnabled(
        events,
        BillingEventType.of(BillingEventType.Dimension.EGRESS_BYTES, internal),
        modelUsage.requestBytes(),
        region,
        resourceId,
        providerName,
        modelName);
    addEventIfEnabled(
        events,
        BillingEventType.of(BillingEventType.Dimension.INGRESS_BYTES, internal),
        modelUsage.responseBytes(),
        region,
        resourceId,
        providerName,
        modelName);
    return events;
  }

  private void addEventIfEnabled(
      List<BillingEvent> events,
      BillingEventType eventType,
      long usage,
      String region,
      String resourceId,
      String providerName,
      String modelName) {
    if (!enabledEventTypes.contains(eventType)) {
      return;
    }
    var properties =
        new BillingEvent.BillingProperties(
            usage, region, resourceType, resourceId, providerName, modelName);
    events.add(new BillingEvent(UUID.randomUUID(), Instant.now(), product, eventType, properties));
  }

  private static String requireNonBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be null or blank");
    }
    return value;
  }
}
