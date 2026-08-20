package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import java.util.List;
import java.util.Objects;

/**
 * Per-request billing sink for model calls.
 *
 * <p>Get an instance via {@code requestContext.billing()}, which delegates to {@link
 * #create(BillingConfig, ApiFeatures)} to pick the right implementation for the request:
 *
 * <ul>
 *   <li>{@link DefaultBilling} — when {@link ApiFeature#BILLING_EVENTS_LOGGING} and/or {@link
 *       ApiFeature#BILLING_EVENTS_RESPONSE} is enabled. It emits structured JSON log lines on the
 *       {@code billing.events} logger (logging flag) and/or buffers events in memory to be returned
 *       on the {@code Billing-Events} HTTP response header (response flag). The two flags are
 *       independent — both can be on at once.
 *   <li>{@link #NO_OP} — when both features are disabled, or in tests / contexts where billing is
 *       not exercised.
 * </ul>
 *
 * Pass each aggregated {@link ModelUsage} to {@link #emitEvent(ModelUsage)}. {@code modelUsage}
 * must be non-null — callers are responsible for ensuring usage data exists before invoking.
 */
public interface Billing {

  /**
   * Emits billing events for the given aggregated model usage.
   *
   * @param modelUsage usage data for the model call; must not be null.
   */
  void emitEvent(ModelUsage modelUsage);

  /**
   * Snapshot of billing events buffered by {@link #emitEvent(ModelUsage)} for this request when
   * {@link ApiFeature#BILLING_EVENTS_RESPONSE} is enabled, read later by {@code
   * BillingResponseFilter} to populate the {@code Billing-Events} response header. Implementations
   * that do not buffer (e.g. {@link #NO_OP}, or {@link DefaultBilling} with the response flag off)
   * return an empty list. The returned list is an unmodifiable copy so callers can iterate safely
   * while other tasks may still be writing.
   */
  default List<BillingEvent> collectedEvents() {
    return List.of();
  }

  /**
   * Shared NO-OP {@link Billing}. Still enforces the non-null {@code modelUsage} contract so tests
   * (and the feature-disabled production path) don't accidentally mask null-passing bugs in calling
   * code.
   */
  Billing NO_OP = modelUsage -> Objects.requireNonNull(modelUsage, "modelUsage must not be null");

  /**
   * Factory that picks the right {@link Billing} implementation for the current request.
   * Centralizes the {@code DefaultBilling vs NO_OP} dispatch so callers (e.g. {@link
   * io.stargate.sgv2.jsonapi.api.request.RequestContext}) don't have to know the rule. Returns
   * {@link DefaultBilling} when {@link ApiFeature#BILLING_EVENTS_LOGGING} and/or {@link
   * ApiFeature#BILLING_EVENTS_RESPONSE} is enabled, telling it which sinks to feed; otherwise
   * {@link #NO_OP}. Reads {@code config} only when a feature is enabled — when both are disabled it
   * is fine to pass any value, including one that would not validate as a real config.
   *
   * @param config billing configuration; only consulted when a feature is enabled
   * @param apiFeatures the request's resolved feature set; must not be null
   */
  static Billing create(BillingConfig config, ApiFeatures apiFeatures) {
    Objects.requireNonNull(apiFeatures, "apiFeatures must not be null");
    boolean loggingEnabled = apiFeatures.isFeatureEnabled(ApiFeature.BILLING_EVENTS_LOGGING);
    boolean responseEnabled = apiFeatures.isFeatureEnabled(ApiFeature.BILLING_EVENTS_RESPONSE);
    return (loggingEnabled || responseEnabled)
        ? new DefaultBilling(config, loggingEnabled, responseEnabled)
        : NO_OP;
  }
}
