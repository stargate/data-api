package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.vertx.core.MultiMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link Billing} interface itself: the {@link Billing#NO_OP} singleton and the
 * {@link Billing#create(BillingConfig, ApiFeatures)} dispatch. Implementation-specific behavior of
 * {@link DefaultBilling} (event building, properties etc.) is covered in {@link
 * DefaultBillingTest}.
 */
class BillingTest {

  private final TestConstants testConstants = new TestConstants();

  // ============================================================
  // NO_OP behavior
  // ============================================================

  @Test
  void noOpDoesNothingForValidUsage() {
    assertThatCode(() -> Billing.NO_OP.emitEvent(stubUsage())).doesNotThrowAnyException();
  }

  @Test
  void noOpThrowsOnNullUsage() {
    // Even the NO-OP enforces the non-null contract so callers can't accidentally pass null and
    // have it silently swallowed in a test that uses the no-op.
    assertThatThrownBy(() -> Billing.NO_OP.emitEvent(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("modelUsage");
  }

  // ============================================================
  // create() dispatch
  // ============================================================

  @Test
  void createReturnsDefaultBillingWhenFeatureEnabled() {
    var billing = Billing.create(validConfig(), featuresWithBilling(true));

    assertThat(billing).isInstanceOf(DefaultBilling.class);
  }

  @Test
  void createReturnsNoOpWhenFeatureDisabled() {
    // BillingConfig isn't consulted when the feature is off — pass null to assert that explicitly.
    var billing = Billing.create(null, featuresWithBilling(false));

    assertThat(billing).isSameAs(Billing.NO_OP);
  }

  // ============================================================
  // Feature-flag precedence at dispatch time
  // ============================================================

  /**
   * If BILLING_EVENTS_LOGGING is enabled in startup config, a request header MUST NOT be able to
   * turn it off — the config is authoritative. Verified at the dispatch layer because that's where
   * the user-visible effect lands (you get DefaultBilling, not NO_OP).
   */
  @Test
  void createConfigEnabledIsNotOverriddenByHeader() {
    var config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of("billing-events-logging", "true"));

    var headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING_EVENTS_LOGGING.httpHeaderName(), "false");
    var apiFeatures =
        ApiFeatures.fromConfigAndRequest(config, new RequestContext.HttpHeaderAccess(headers));

    var billing = Billing.create(validConfig(), apiFeatures);

    assertThat(billing)
        .as("config=true must win over header=false at dispatch")
        .isInstanceOf(DefaultBilling.class);
  }

  /**
   * Conversely, if startup config leaves the flag unset, a request header CAN enable it. Proves the
   * header path is alive — without this, the test above would pass trivially.
   */
  @Test
  void createHeaderEnablesWhenConfigUnset() {
    var config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of());

    var headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING_EVENTS_LOGGING.httpHeaderName(), "true");
    var apiFeatures =
        ApiFeatures.fromConfigAndRequest(config, new RequestContext.HttpHeaderAccess(headers));

    var billing = Billing.create(validConfig(), apiFeatures);

    assertThat(billing).isInstanceOf(DefaultBilling.class);
  }

  // ============================================================
  // Helpers
  // ============================================================

  /** Minimal valid {@link BillingConfig} — enough for {@link DefaultBilling} to construct. */
  private static BillingConfig validConfig() {
    var config = mock(BillingConfig.class);
    when(config.product()).thenReturn("serverless");
    when(config.resourceType()).thenReturn("serverless_database");
    when(config.internalModelProviders()).thenReturn(List.of("nvidia"));
    when(config.enabledEventTypes()).thenReturn(Optional.empty());
    return config;
  }

  private static ApiFeatures featuresWithBilling(boolean enabled) {
    var config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of("billing-events-logging", String.valueOf(enabled)));
    return ApiFeatures.fromConfigAndRequest(config, null);
  }

  private ModelUsage stubUsage() {
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "model-x",
        testConstants.TENANT,
        ModelInputType.INDEX,
        100,
        500,
        256,
        12_345,
        1000L);
  }
}
