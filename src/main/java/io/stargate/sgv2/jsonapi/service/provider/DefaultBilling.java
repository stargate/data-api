package io.stargate.sgv2.jsonapi.service.provider;

import static io.stargate.sgv2.jsonapi.util.StringUtil.requireNonBlank;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Billing} implementation that dispatches each built event to one or both sinks selected at
 * construction time:
 *
 * <ul>
 *   <li>structured JSON log lines on the {@code billing.events} logger for downstream billing
 *       pipelines — when {@code loggingEnabled} ({@link ApiFeature#BILLING_EVENTS_LOGGING}).
 *   <li>an in-memory buffer surfaced via {@link #collectedEvents()} and returned on the {@code
 *       Billing-Events} HTTP response header — when {@code responseEnabled} ({@link
 *       ApiFeature#BILLING_EVENTS_RESPONSE}).
 * </ul>
 *
 * <p>Construction is driven by {@link Billing#create} (via {@link
 * io.stargate.sgv2.jsonapi.api.request.RequestContext#billing()}), which only picks this
 * implementation when at least one of the two flags is enabled and tells it which sinks to feed.
 * This class therefore does not re-check the feature flags; if a flag is off, its sink is skipped.
 *
 * <p>For each {@link ModelUsage}, up to three events are emitted, one per billable metric ({@link
 * BillingEventType.Metric#TOTAL_TOKENS TOTAL_TOKENS}, {@link BillingEventType.Metric#EGRESS_BYTES
 * EGRESS_BYTES}, {@link BillingEventType.Metric#INGRESS_BYTES INGRESS_BYTES}). The {@code
 * internal_*} variant is used when the model provider is listed in {@link
 * BillingConfig#internalModelProviders()}; otherwise the {@code external_*} variant is used. Events
 * whose type is not in {@link BillingConfig#enabledEventTypes()} are dropped.
 *
 * <p>{@link #emitEvent(ModelUsage)} requires a non-null {@link ModelUsage} — callers must guarantee
 * usage data exists before invoking. The region for each event is read from {@link
 * ModelUsage#tenant()} ({@code Tenant.region()}).
 */
public class DefaultBilling implements Billing {

  private static final Logger BILLING_LOGGER = LoggerFactory.getLogger("billing.events");
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBilling.class);

  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

  private final String product;
  private final String resourceType;
  private final Set<String> internalModelProviders;
  private final Set<BillingEventType> enabledEventTypes;

  /** Whether to emit events on the {@code billing.events} logger. */
  private final boolean loggingEnabled;

  /** Whether to buffer events in {@link #collectedEvents} for the response header. */
  private final boolean responseEnabled;

  // Events buffered for the BILLING_EVENTS_RESPONSE sink. Populated only when responseEnabled.
  // emitEvent can be invoked from concurrent tasks within one request (async embedding / reranking
  // calls), so the list is synchronized.
  private final List<BillingEvent> collectedEvents =
      Collections.synchronizedList(new ArrayList<>());

  public DefaultBilling(BillingConfig config, boolean loggingEnabled, boolean responseEnabled) {
    Objects.requireNonNull(config, "config must not be null");
    this.product = requireNonBlank(config.product(), "billing.product");
    this.resourceType = requireNonBlank(config.resourceType(), "billing.resource_type");
    this.internalModelProviders = Set.copyOf(config.internalModelProviders());
    this.enabledEventTypes =
        config.enabledEventTypes().map(Set::copyOf).orElse(BillingEventType.ALL);
    this.loggingEnabled = loggingEnabled;
    this.responseEnabled = responseEnabled;
  }

  /**
   * Builds billing events for the given aggregated model usage and dispatches them to whichever
   * sinks are enabled: the {@code billing.events} logger ({@code loggingEnabled}) and/or the
   * in-memory buffer read via {@link #collectedEvents()} ({@code responseEnabled}). The {@code
   * billing.events} logger level is also checked so we skip the log sink when the logger is
   * silenced at runtime; if no sink is active, event construction is skipped entirely.
   *
   * @param modelUsage usage data for the model call; must not be null. Callers are expected to
   *     ensure they have usage data before invoking.
   */
  @Override
  public void emitEvent(ModelUsage modelUsage) {
    Objects.requireNonNull(modelUsage, "modelUsage must not be null");
    boolean shouldLog = loggingEnabled && BILLING_LOGGER.isInfoEnabled();
    if (!shouldLog && !responseEnabled) {
      return;
    }
    var events = buildEvents(modelUsage);
    if (shouldLog) {
      for (var event : events) {
        try {
          BILLING_LOGGER.info(OBJECT_WRITER.writeValueAsString(event));
        } catch (JacksonException e) {
          LOGGER.error("Failed to serialize billing event of type {}", event.eventType(), e);
        }
      }
    }
    if (responseEnabled) {
      collectedEvents.addAll(events);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns an unmodifiable copy of the events buffered so far for the {@code
   * BILLING_EVENTS_RESPONSE} sink; empty when {@code responseEnabled} is false.
   */
  @Override
  public List<BillingEvent> collectedEvents() {
    synchronized (collectedEvents) {
      return List.copyOf(collectedEvents);
    }
  }

  /**
   * Builds the list of billing events for one {@link ModelUsage}: one event per billable metric
   * (total tokens, egress bytes, ingress bytes), with the {@code internal_*} or {@code external_*}
   * variant chosen based on {@link BillingConfig#internalModelProviders()}. Events whose type is
   * not in {@link BillingConfig#enabledEventTypes()} are filtered out. All events from a single
   * {@code ModelUsage} share one timestamp so they can be correlated in the billing logs.
   */
  @VisibleForTesting
  List<BillingEvent> buildEvents(ModelUsage modelUsage) {
    var internal = internalModelProviders.contains(modelUsage.modelProvider().apiName());
    var region = modelUsage.tenant().region();
    var resourceId = modelUsage.tenant().toString();
    var providerName = modelUsage.modelProvider().apiName();
    var modelName = modelUsage.modelName();
    var timestamp = Instant.now();

    var events = new ArrayList<BillingEvent>(3);
    addEventIfEnabled(
        events,
        BillingEventType.of(BillingEventType.Metric.TOTAL_TOKENS, internal),
        modelUsage.totalTokens(),
        region,
        resourceId,
        providerName,
        modelName,
        timestamp);
    addEventIfEnabled(
        events,
        BillingEventType.of(BillingEventType.Metric.EGRESS_BYTES, internal),
        modelUsage.requestBytes(),
        region,
        resourceId,
        providerName,
        modelName,
        timestamp);
    addEventIfEnabled(
        events,
        BillingEventType.of(BillingEventType.Metric.INGRESS_BYTES, internal),
        modelUsage.responseBytes(),
        region,
        resourceId,
        providerName,
        modelName,
        timestamp);
    return events;
  }

  private void addEventIfEnabled(
      List<BillingEvent> events,
      BillingEventType eventType,
      long usage,
      String region,
      String resourceId,
      String providerName,
      String modelName,
      Instant timestamp) {
    if (!enabledEventTypes.contains(eventType)) {
      return;
    }
    var properties =
        new BillingEvent.BillingProperties(
            usage, region, resourceType, resourceId, providerName, modelName);
    events.add(new BillingEvent(UUID.randomUUID(), timestamp, product, eventType, properties));
  }
}
