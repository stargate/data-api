package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import java.util.Objects;

/**
 * Per-request billing sink for model calls.
 *
 * <p>Get an instance via {@code requestContext.billing()}, which delegates to {@link
 * #create(BillingConfig, ApiFeatures)} to pick the right implementation for the request:
 *
 * <ul>
 *   <li>{@link DefaultBilling} — when {@link ApiFeature#BILLING_EVENTS_LOGGING} is enabled; emits
 *       structured JSON log lines on the {@code billing.events} logger.
 *   <li>{@link #NO_OP} — when the feature is disabled, or in tests / contexts where billing is not
 *       exercised.
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
   * Shared NO-OP {@link Billing}. Still enforces the non-null {@code modelUsage} contract so tests
   * (and the feature-disabled production path) don't accidentally mask null-passing bugs in calling
   * code.
   */
  Billing NO_OP = modelUsage -> Objects.requireNonNull(modelUsage, "modelUsage must not be null");

  /**
   * Factory that picks the right {@link Billing} implementation for the current request.
   * Centralizes the {@code DefaultBilling vs NO_OP} dispatch so callers (e.g. {@link
   * io.stargate.sgv2.jsonapi.api.request.RequestContext}) don't have to know the rule. Reads {@code
   * config} only when the feature is enabled — when disabled it is fine to pass any value,
   * including one that would not validate as a real config.
   *
   * @param config billing configuration; only consulted when the feature is enabled
   * @param apiFeatures the request's resolved feature set; must not be null
   */
  static Billing create(BillingConfig config, ApiFeatures apiFeatures) {
    Objects.requireNonNull(apiFeatures, "apiFeatures must not be null");
    return apiFeatures.isFeatureEnabled(ApiFeature.BILLING_EVENTS_LOGGING)
        ? new DefaultBilling(config)
        : NO_OP;
  }
}
