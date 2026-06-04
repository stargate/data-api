package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
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

  // ============================================================
  // NO_OP behavior
  // ============================================================

  @Test
  void noOp_doesNothingForValidUsage() {
    // Should run without side effects or exceptions.
    assertThatCode(() -> Billing.NO_OP.emitEvent(stubUsage())).doesNotThrowAnyException();
  }

  @Test
  void noOp_throwsOnNullUsage() {
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
  void create_returnsDefaultBillingWhenFeatureEnabled() {
    Billing billing = Billing.create(validConfig(), featuresWithBilling(true));

    assertThat(billing).isInstanceOf(DefaultBilling.class);
  }

  @Test
  void create_returnsNoOpWhenFeatureDisabled() {
    // BillingConfig isn't consulted when the feature is off — pass null to assert that explicitly.
    Billing billing = Billing.create(null, featuresWithBilling(false));

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
  void create_configEnabledIsNotOverriddenByHeader() {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of(ApiFeature.BILLING_EVENTS_LOGGING, "true"));

    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING_EVENTS_LOGGING.httpHeaderName(), "false");
    var apiFeatures =
        ApiFeatures.fromConfigAndRequest(config, new RequestContext.HttpHeaderAccess(headers));

    Billing billing = Billing.create(validConfig(), apiFeatures);

    assertThat(billing)
        .as("config=true must win over header=false at dispatch")
        .isInstanceOf(DefaultBilling.class);
  }

  /**
   * Conversely, if startup config leaves the flag unset, a request header CAN enable it. Proves the
   * header path is alive — without this, the test above would pass trivially.
   */
  @Test
  void create_headerEnablesWhenConfigUnset() {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of());

    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING_EVENTS_LOGGING.httpHeaderName(), "true");
    var apiFeatures =
        ApiFeatures.fromConfigAndRequest(config, new RequestContext.HttpHeaderAccess(headers));

    Billing billing = Billing.create(validConfig(), apiFeatures);

    assertThat(billing).isInstanceOf(DefaultBilling.class);
  }

  // ============================================================
  // Helpers
  // ============================================================

  /** Minimal valid {@link BillingConfig} — enough for {@link DefaultBilling} to construct. */
  private static BillingConfig validConfig() {
    BillingConfig config = mock(BillingConfig.class);
    when(config.product()).thenReturn("serverless");
    when(config.resourceType()).thenReturn("serverless_database");
    when(config.internalModelProviders()).thenReturn(List.of("nvidia"));
    when(config.enabledEventTypes()).thenReturn(Optional.empty());
    return config;
  }

  private static ApiFeatures featuresWithBilling(boolean enabled) {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags())
        .thenReturn(Map.of(ApiFeature.BILLING_EVENTS_LOGGING, String.valueOf(enabled)));
    return ApiFeatures.fromConfigAndRequest(config, null);
  }

  private static ModelUsage stubUsage() {
    Tenant tenant = Tenant.create(DatabaseType.ASTRA, "tenant-x", "us-west-2");
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "model-x",
        tenant,
        ModelInputType.INDEX,
        100,
        500,
        256,
        12_345,
        1000L);
  }
}
