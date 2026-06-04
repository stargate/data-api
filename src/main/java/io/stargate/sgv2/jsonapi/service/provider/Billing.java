package io.stargate.sgv2.jsonapi.service.provider;

import java.util.Objects;

/**
 * Per-request billing sink for model calls.
 *
 * <p>Get an instance via {@code requestContext.billing()}. The request context decides at
 * construction time which implementation to hand out:
 *
 * <ul>
 *   <li>{@link DefaultBilling} — when {@link
 *       io.stargate.sgv2.jsonapi.config.feature.ApiFeature#BILLING_EVENTS_LOGGING} is enabled;
 *       emits structured JSON log lines on the {@code billing.events} logger.
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
}
