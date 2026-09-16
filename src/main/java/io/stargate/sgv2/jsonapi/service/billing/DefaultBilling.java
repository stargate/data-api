package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.util.StringUtil.requireNonBlank;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Billing} implementation that emits structured JSON log lines on the {@code billing.events}
 * logger for downstream billing pipelines.
 *
 * <p>For each {@link ModelUsage}, up to three events are emitted, for the egress and ingress bytes
 * of the request and the total number of tokens used. See {@link BillingEventType} for which ones
 * should be used.
 *
 * <p><b>NOTE:</b> this class relies on the configured format for the billing logger called {@link
 * #BILLING_LOGGER_NAME}, it should only log the message on the line because that has the JSON of
 * the {@link BillingEvent}
 */
public class DefaultBilling implements Billing {

  /** Name of the logger that billing events will be sent to. */
  public static final String BILLING_LOGGER_NAME = "billing.events";

  private static final Logger BILLING_LOGGER = LoggerFactory.getLogger(BILLING_LOGGER_NAME);
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBilling.class);

  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

  // There are three metrics, but 6 Event Types, because 3 each for internal or external provider.
  // this is the mapping of the metric and where we get it from
  private static final List<Pair<BillingEventType.Metric, Function<ModelUsage, Integer>>>
      METRICS_PER_USAGE =
          List.of(
              Pair.of(BillingEventType.Metric.TOTAL_TOKENS, ModelUsage::totalTokens),
              Pair.of(BillingEventType.Metric.EGRESS_BYTES, ModelUsage::requestBytes),
              Pair.of(BillingEventType.Metric.INGRESS_BYTES, ModelUsage::responseBytes));

  private final String product;
  private final String resourceType;
  private final Set<String> internalModelProviders;
  private final Set<BillingEventType> enabledEventTypes;

  public DefaultBilling(BillingConfig config) {
    Objects.requireNonNull(config, "config must not be null");

    this.product = requireNonBlank(config.product(), "billing.product");
    this.resourceType = requireNonBlank(config.resourceType(), "billing.resource_type");
    this.internalModelProviders = Set.copyOf(config.internalModelProviders());
    this.enabledEventTypes =
        config.enabledEventTypes().map(Set::copyOf).orElse(BillingEventType.ALL);
  }

  /**
   * Emits billing events for the given aggregated model usage.
   *
   * @param modelUsage usage data for the model call; must not be null.
   */
  @Override
  public void emitEvent(ModelUsage modelUsage) {
    Objects.requireNonNull(modelUsage, "modelUsage must not be null");

    if (!BILLING_LOGGER.isInfoEnabled()) {
      return;
    }

    for (var event : buildEvents(modelUsage)) {
      try {
        BILLING_LOGGER.info(OBJECT_WRITER.writeValueAsString(event));
      } catch (JacksonException e) {
        LOGGER.error("Failed to serialize billing event of type {}", event.eventType(), e);
      }
    }
  }

  /**
   * Builds the list of billing events for one {@link ModelUsage}: one event per billable metric
   * (total tokens, egress bytes, ingress bytes), with the {@code internal_*} or {@code external_*}
   * variant chosen based on {@link BillingConfig#internalModelProviders()}.
   *
   * <p>All events from a single {@code ModelUsage} share one timestamp so they can be correlated in
   * the billing logs.
   */
  @VisibleForTesting
  List<BillingEvent> buildEvents(ModelUsage modelUsage) {

    var internal = internalModelProviders.contains(modelUsage.modelProvider().apiName());
    var region = modelUsage.tenant().region();
    var resourceId = modelUsage.tenant().toString();
    var providerName = modelUsage.modelProvider().apiName();
    var modelName = modelUsage.modelName();
    var modelType = modelUsage.modelType().apiName();
    var timestamp = Instant.now();
    var events = new ArrayList<BillingEvent>(3);

    for (Pair<BillingEventType.Metric, Function<ModelUsage, Integer>> pair : METRICS_PER_USAGE) {

      var metric = pair.getLeft();
      var eventType = BillingEventType.of(metric, internal);
      var supplier = pair.getRight();

      if (!enabledEventTypes.contains(eventType)) {
        continue;
      }

      var properties =
          new BillingEvent.BillingProperties(
              supplier.apply(modelUsage),
              region,
              resourceType,
              resourceId,
              providerName,
              modelName,
              modelType);
      events.add(new BillingEvent(UUID.randomUUID(), timestamp, product, eventType, properties));
    }
    return events;
  }
}
