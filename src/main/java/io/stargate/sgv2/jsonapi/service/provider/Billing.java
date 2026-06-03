package io.stargate.sgv2.jsonapi.service.provider;

import java.util.Objects;

/**
 * Per-request billing sink for model calls. The production implementation, {@link LoggingBilling},
 * emits structured JSON log lines on the {@code billing.events} logger for downstream pipelines.
 *
 * <p>Get an instance via {@code requestContext.billing()}. Pass each aggregated {@link ModelUsage}
 * to {@link #emitEvent(ModelUsage)}. {@code modelUsage} must be non-null — callers are responsible
 * for ensuring usage data exists before invoking. A null would silently hide a calling-side bug, so
 * implementations fail loudly.
 *
 * <p>Use {@link #NO_OP} in tests (and any context where billing is not exercised) to skip the
 * config-mocking required to instantiate {@link LoggingBilling}.
 */
public interface Billing {

  /**
   * Emits billing events for the given aggregated model usage, subject to the implementation's own
   * gating (e.g. {@link LoggingBilling} also checks the feature flag and logger).
   *
   * @param modelUsage usage data for the model call; must not be null.
   */
  void emitEvent(ModelUsage modelUsage);

  /**
   * Shared NO-OP {@link Billing}. Still enforces the non-null {@code modelUsage} contract so tests
   * using it don't accidentally mask null-passing bugs in calling code.
   */
  Billing NO_OP = modelUsage -> Objects.requireNonNull(modelUsage, "modelUsage must not be null");
}
