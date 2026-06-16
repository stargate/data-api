package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.EnumSet;
import java.util.Set;

/**
 * The set of billing event types emitted by {@link Billing}.
 *
 * <p>Each event represents a single billable metric of a model call. The {@code internal_*}
 * variants are emitted for providers configured in {@link
 * io.stargate.sgv2.jsonapi.config.BillingConfig#internalModelProviders()}; everything else is
 * {@code external_*}.
 *
 * <ul>
 *   <li>{@link #INTERNAL_MODEL_TOTAL_TOKENS} / {@link #EXTERNAL_MODEL_TOTAL_TOKENS} — total tokens
 *       reported by the model.
 *   <li>{@link #INTERNAL_MODEL_EGRESS_BYTES} / {@link #EXTERNAL_MODEL_EGRESS_BYTES} — bytes sent
 *       from the data plane to the model (request payload).
 *   <li>{@link #INTERNAL_MODEL_INGRESS_BYTES} / {@link #EXTERNAL_MODEL_INGRESS_BYTES} — bytes
 *       received from the model back to the data plane (response payload).
 * </ul>
 */
public enum BillingEventType {
  INTERNAL_MODEL_TOTAL_TOKENS("internal_model_total_tokens", true, Metric.TOTAL_TOKENS),
  EXTERNAL_MODEL_TOTAL_TOKENS("external_model_total_tokens", false, Metric.TOTAL_TOKENS),
  INTERNAL_MODEL_EGRESS_BYTES("internal_model_egress_bytes", true, Metric.EGRESS_BYTES),
  EXTERNAL_MODEL_EGRESS_BYTES("external_model_egress_bytes", false, Metric.EGRESS_BYTES),
  INTERNAL_MODEL_INGRESS_BYTES("internal_model_ingress_bytes", true, Metric.INGRESS_BYTES),
  EXTERNAL_MODEL_INGRESS_BYTES("external_model_ingress_bytes", false, Metric.INGRESS_BYTES);

  /** The billable metric a {@link BillingEventType} measures. */
  public enum Metric {
    TOTAL_TOKENS,
    EGRESS_BYTES,
    INGRESS_BYTES
  }

  public static final Set<BillingEventType> ALL = Set.copyOf(EnumSet.allOf(BillingEventType.class));

  private final String eventName;
  private final boolean internal;
  private final Metric metric;

  BillingEventType(String eventName, boolean internal, Metric metric) {
    // Event names are emitted lower-case in the JSON billing event payload; normalize defensively
    // so a stray uppercase character in the literal can't break downstream consumers.
    this.eventName = eventName.toLowerCase();
    this.internal = internal;
    this.metric = metric;
  }

  /** Lower-case event_type string used in the JSON billing event. */
  @JsonValue
  public String eventName() {
    return eventName;
  }

  public Metric metric() {
    return metric;
  }

  /**
   * Resolves the event type for a given metric and provider classification.
   *
   * @param metric which billable metric we are emitting
   * @param internal {@code true} if the model provider is configured as internal
   */
  public static BillingEventType of(Metric metric, boolean internal) {
    return switch (metric) {
      case TOTAL_TOKENS -> internal ? INTERNAL_MODEL_TOTAL_TOKENS : EXTERNAL_MODEL_TOTAL_TOKENS;
      case EGRESS_BYTES -> internal ? INTERNAL_MODEL_EGRESS_BYTES : EXTERNAL_MODEL_EGRESS_BYTES;
      case INGRESS_BYTES -> internal ? INTERNAL_MODEL_INGRESS_BYTES : EXTERNAL_MODEL_INGRESS_BYTES;
    };
  }
}
