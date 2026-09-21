package io.stargate.sgv2.jsonapi.service.billing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelType;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import io.stargate.sgv2.jsonapi.util.ApiFeaturesTestUtil;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link Billing} interface itself: the {@link Billing#NO_OP} singleton and the
 * {@link Billing#create(BillingConfig, ApiFeatures)} dispatch.
 *
 * <p>See also {@link DefaultBillingTest}
 */
class BillingTest {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  // ============================================================
  // NO_OP behavior
  // ============================================================

  @Test
  void noOpDoesNothingForValidUsage() {
    assertThatCode(() -> Billing.NO_OP.emitEvent(stubUsage())).doesNotThrowAnyException();
  }

  /**
   * Even the NO-OP enforces the non-null contract so callers can't accidentally pass null and have
   * it silently swallowed in a test that uses the no-op.
   */
  @Test
  void noOpThrowsOnNullUsage() {
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
   * turn it off.
   */
  @Test
  void createConfigEnabledIsNotOverriddenByHeader() {

    // enabled in the config
    // disabled by the header
    var apiFeatures = featuresWithBilling(true, false);

    var billing = Billing.create(validConfig(), apiFeatures);

    assertThat(billing)
        .as("config=true must win over header=false")
        .isInstanceOf(DefaultBilling.class);
  }

  /**
   * Conversely, if startup config leaves the flag unset, a request header CAN enable it. Proves the
   * header path is alive
   */
  @Test
  void createHeaderEnablesWhenConfigUnset() {

    // no value  in the config
    // enabled by the header
    var apiFeatures = featuresWithBilling(null, true);

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
    return featuresWithBilling(enabled, null);
  }

  private static ApiFeatures featuresWithBilling(Boolean configEnabled, Boolean headerEnabled) {
    return ApiFeaturesTestUtil.withFeature(
        ApiFeature.BILLING_EVENTS_LOGGING, configEnabled, headerEnabled);
  }

  private ModelUsage stubUsage() {
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "model-x",
        TEST_CONSTANTS.TENANT,
        ModelInputType.INDEX,
        100,
        500,
        256,
        12_345,
        1000L);
  }
}
