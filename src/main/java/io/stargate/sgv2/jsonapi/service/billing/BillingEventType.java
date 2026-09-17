package io.stargate.sgv2.jsonapi.service.billing;

import com.fasterxml.jackson.annotation.JsonValue;
import io.stargate.sgv2.jsonapi.service.provider.ModelType;
import java.util.EnumSet;
import java.util.Set;

/**
 * The set of billing event types emitted by {@link Billing}.
 *
 * <p>Each event represents a single billable metric of a model call. The {@code internal_*}
 * variants are emitted for providers configured in {@link
 * io.stargate.sgv2.jsonapi.config.BillingConfig#internalModelProviders()}; everything else is
 * {@code external_*}. Each has an {@code embedding} and a {@code reranking} variant, from the
 * {@link ModelType} of the call.
 *
 * <ul>
 *   <li>{@code *_TOTAL_TOKENS} — total tokens reported by the model.
 *   <li>{@code *_EGRESS_BYTES} — bytes sent from the data plane to the model (request payload).
 *   <li>{@code *_INGRESS_BYTES} — bytes received from the model back to the data plane (response
 *       payload).
 * </ul>
 */
public enum BillingEventType {
  INTERNAL_EMBEDDING_TOTAL_TOKENS(true, ModelType.EMBEDDING, Metric.TOTAL_TOKENS),
  INTERNAL_RERANKING_TOTAL_TOKENS(true, ModelType.RERANKING, Metric.TOTAL_TOKENS),
  EXTERNAL_EMBEDDING_TOTAL_TOKENS(false, ModelType.EMBEDDING, Metric.TOTAL_TOKENS),
  EXTERNAL_RERANKING_TOTAL_TOKENS(false, ModelType.RERANKING, Metric.TOTAL_TOKENS),

  INTERNAL_EMBEDDING_EGRESS_BYTES(true, ModelType.EMBEDDING, Metric.EGRESS_BYTES),
  INTERNAL_RERANKING_EGRESS_BYTES(true, ModelType.RERANKING, Metric.EGRESS_BYTES),
  EXTERNAL_EMBEDDING_EGRESS_BYTES(false, ModelType.EMBEDDING, Metric.EGRESS_BYTES),
  EXTERNAL_RERANKING_EGRESS_BYTES(false, ModelType.RERANKING, Metric.EGRESS_BYTES),

  INTERNAL_EMBEDDING_INGRESS_BYTES(true, ModelType.EMBEDDING, Metric.INGRESS_BYTES),
  INTERNAL_RERANKING_INGRESS_BYTES(true, ModelType.RERANKING, Metric.INGRESS_BYTES),
  EXTERNAL_EMBEDDING_INGRESS_BYTES(false, ModelType.EMBEDDING, Metric.INGRESS_BYTES),
  EXTERNAL_RERANKING_INGRESS_BYTES(false, ModelType.RERANKING, Metric.INGRESS_BYTES);

  /** The billable metric a {@link BillingEventType} measures. */
  public enum Metric {
    TOTAL_TOKENS("total_tokens"),
    EGRESS_BYTES("egress_bytes"),
    INGRESS_BYTES("ingress_bytes");

    private final String billingEventName;

    Metric(String billingEventName) {
      this.billingEventName = billingEventName;
    }

    /** Name of the metric used in the billing event name. */
    public String billingEventName() {
      return billingEventName;
    }
  }

  public static final Set<BillingEventType> ALL = Set.copyOf(EnumSet.allOf(BillingEventType.class));

  private static final String INTERNAL = "internal";
  private static final String EXTERNAL = "external";

  private final String eventName;
  private final boolean internal;
  private final ModelType modelType;
  private final Metric metric;

  BillingEventType(boolean internal, ModelType modelType, Metric metric) {
    this.eventName = eventName(internal, modelType, metric);
    this.internal = internal;
    this.modelType = modelType;
    this.metric = metric;
  }

  /** Builds the event name e.g. {@code internal_embedding_total_tokens} */
  private static String eventName(boolean internal, ModelType modelType, Metric metric) {
    return String.join(
        "_",
        internal ? INTERNAL : EXTERNAL,
        modelType.billingEventName(),
        metric.billingEventName());
  }

  /** Lower-case event_type string used in the JSON billing event. */
  @JsonValue
  public String eventName() {
    return eventName;
  }

  public ModelType modelType() {
    return modelType;
  }

  public Metric metric() {
    return metric;
  }

  /**
   * Resolves the event type for a given model type, metric and provider classification.
   *
   * @param modelType the type of model that was called
   * @param metric which billable metric we are emitting
   * @param internal {@code true} if the model provider is configured as internal
   * @throws IllegalArgumentException if the modelType is {@link ModelType#MODEL_TYPE_UNSPECIFIED}
   */
  public static BillingEventType of(ModelType modelType, Metric metric, boolean internal) {
    return switch (modelType) {
      case MODEL_TYPE_UNSPECIFIED ->
          throw new IllegalArgumentException(
              "BillingEventType.of() - modelType must be specified, modelType=%s, metric=%s"
                  .formatted(modelType, metric));
      case EMBEDDING ->
          switch (metric) {
            case TOTAL_TOKENS ->
                internal ? INTERNAL_EMBEDDING_TOTAL_TOKENS : EXTERNAL_EMBEDDING_TOTAL_TOKENS;
            case EGRESS_BYTES ->
                internal ? INTERNAL_EMBEDDING_EGRESS_BYTES : EXTERNAL_EMBEDDING_EGRESS_BYTES;
            case INGRESS_BYTES ->
                internal ? INTERNAL_EMBEDDING_INGRESS_BYTES : EXTERNAL_EMBEDDING_INGRESS_BYTES;
          };
      case RERANKING ->
          switch (metric) {
            case TOTAL_TOKENS ->
                internal ? INTERNAL_RERANKING_TOTAL_TOKENS : EXTERNAL_RERANKING_TOTAL_TOKENS;
            case EGRESS_BYTES ->
                internal ? INTERNAL_RERANKING_EGRESS_BYTES : EXTERNAL_RERANKING_EGRESS_BYTES;
            case INGRESS_BYTES ->
                internal ? INTERNAL_RERANKING_INGRESS_BYTES : EXTERNAL_RERANKING_INGRESS_BYTES;
          };
    };
  }
}
