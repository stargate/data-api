package io.stargate.sgv2.jsonapi.service.billing;

import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import java.util.Objects;

/**
 * Per-request billing sink for model calls.
 *
 * <p>Get an instance via {@code requestContext.billing()}, which delegates to {@link
 * #create(BillingConfig, ApiFeatures)} to pick the right implementation for the request
 *
 * <p>Call {@link #emitEvent(ModelUsage)} to generate a billing event for the model usage, the
 * implementation takes care of where the event goes.
 */
public interface Billing {

  /**
   * Shared NO-OP {@link Billing}. Still enforces the non-null {@code modelUsage} contract so tests
   * (and the feature-disabled production path) don't accidentally mask null-passing bugs in calling
   * code.
   */
  Billing NO_OP = modelUsage -> Objects.requireNonNull(modelUsage, "modelUsage must not be null");

  /**
   * Emits billing events for the given model usage.
   *
   * @param modelUsage usage data for the model call; must not be null.
   */
  void emitEvent(ModelUsage modelUsage);

  /**
   * Factory to create the correct instance of {@link Billing} for a given request.
   *
   * <ul>
   *   <li>{@link DefaultBilling} — when {@link ApiFeature#BILLING_EVENTS_LOGGING} is enabled; emits
   *       structured JSON log lines on the {@code billing.events} logger.
   *   <li>{@link #NO_OP} — when the feature is disabled, or in tests / contexts where billing is
   *       not exercised.
   * </ul>
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
