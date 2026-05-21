package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BillingTest {

  private static final String TENANT_ID = "db-uuid-1234";
  private static final String REGION = "us-west-2";
  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final int TOTAL_TOKENS = 500;
  private static final int RESPONSE_BYTES = 12_345;

  private Billing newBilling(Optional<String> region) {
    BillingConfig config = mock(BillingConfig.class);
    when(config.product()).thenReturn(PRODUCT);
    when(config.resourceType()).thenReturn(RESOURCE_TYPE);
    when(config.region()).thenReturn(region);
    return new Billing(config);
  }

  private ApiFeatures featuresWithBilling(boolean enabled) {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of(ApiFeature.BILLING, String.valueOf(enabled)));
    return ApiFeatures.fromConfigAndRequest(config, null);
  }

  private ModelUsage astraUsage(ModelProvider provider, ModelType modelType) {
    Tenant tenant = Tenant.create(DatabaseType.ASTRA, TENANT_ID);
    return new ModelUsage(
        provider,
        modelType,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        TOTAL_TOKENS,
        0,
        RESPONSE_BYTES,
        1000L);
  }

  static Stream<Arguments> providerCombinations() {
    return Stream.of(
        Arguments.of(ModelProvider.NVIDIA, ModelType.EMBEDDING),
        Arguments.of(ModelProvider.NVIDIA, ModelType.RERANKING),
        Arguments.of(ModelProvider.OPENAI, ModelType.EMBEDDING));
  }

  @ParameterizedTest
  @MethodSource("providerCombinations")
  void buildEvents_emitsTokensAndEgress(ModelProvider provider, ModelType modelType) {
    Billing billing = newBilling(Optional.of(REGION));
    ModelUsage usage = astraUsage(provider, modelType);

    List<BillingEvent> events = billing.buildEvents(usage, REGION);

    assertThat(events).hasSize(2);

    BillingEvent tokens = events.get(0);
    assertThat(tokens.eventType())
        .isEqualTo(provider.apiName() + "_" + modelType.billingName() + "_tokens");
    assertThat(tokens.product()).isEqualTo(PRODUCT);
    assertThat(tokens.properties().usage()).isEqualTo(TOTAL_TOKENS);
    assertThat(tokens.properties().region()).isEqualTo(REGION);
    assertThat(tokens.properties().resourceType()).isEqualTo(RESOURCE_TYPE);
    assertThat(tokens.properties().resourceId()).isEqualTo(TENANT_ID);

    BillingEvent egress = events.get(1);
    assertThat(egress.eventType()).isEqualTo(provider.apiName() + "_egress_bytes");
    assertThat(egress.properties().usage()).isEqualTo(RESPONSE_BYTES);
    assertThat(egress.properties().region()).isEqualTo(REGION);
    assertThat(egress.properties().resourceType()).isEqualTo(RESOURCE_TYPE);
    assertThat(egress.properties().resourceId()).isEqualTo(TENANT_ID);
  }

  @Test
  void shouldEmit_trueForAstraWithFeatureEnabled() {
    Billing billing = newBilling(Optional.of(REGION));
    ModelUsage usage = astraUsage(ModelProvider.NVIDIA, ModelType.EMBEDDING);

    assertThat(billing.shouldEmit(usage, featuresWithBilling(true))).isTrue();
  }

  @Test
  void shouldEmit_falseForNullUsage() {
    Billing billing = newBilling(Optional.of(REGION));

    assertThat(billing.shouldEmit(null, featuresWithBilling(true))).isFalse();
  }

  @Test
  void shouldEmit_falseForNonAstraTenant() {
    Billing billing = newBilling(Optional.of(REGION));
    Tenant tenant = Tenant.create(DatabaseType.CASSANDRA, null);
    ModelUsage usage =
        new ModelUsage(
            ModelProvider.NVIDIA,
            ModelType.EMBEDDING,
            "test-model",
            tenant,
            ModelInputType.INDEX,
            100,
            TOTAL_TOKENS,
            0,
            RESPONSE_BYTES,
            1000L);

    assertThat(billing.shouldEmit(usage, featuresWithBilling(true))).isFalse();
  }

  @Test
  void shouldEmit_falseWhenFeatureDisabled() {
    Billing billing = newBilling(Optional.of(REGION));
    ModelUsage usage = astraUsage(ModelProvider.NVIDIA, ModelType.EMBEDDING);

    assertThat(billing.shouldEmit(usage, featuresWithBilling(false))).isFalse();
  }

  @Test
  void bill_isNoOpWhenGatesFail() {
    // Smoke test: bill() must not throw for any of the skip paths.
    Billing billing = newBilling(Optional.of(REGION));
    billing.bill(null, featuresWithBilling(true));
    billing.bill(astraUsage(ModelProvider.NVIDIA, ModelType.EMBEDDING), featuresWithBilling(false));

    Tenant nonAstra = Tenant.create(DatabaseType.CASSANDRA, null);
    ModelUsage cassandra =
        new ModelUsage(
            ModelProvider.NVIDIA,
            ModelType.EMBEDDING,
            "test-model",
            nonAstra,
            ModelInputType.INDEX,
            100,
            TOTAL_TOKENS,
            0,
            RESPONSE_BYTES,
            1000L);
    billing.bill(cassandra, featuresWithBilling(true));
  }

  @Test
  void bill_isNoOpWhenRegionMissing() {
    // No region configured: bill() logs an error and returns without throwing.
    Billing billing = newBilling(Optional.empty());
    billing.bill(astraUsage(ModelProvider.NVIDIA, ModelType.EMBEDDING), featuresWithBilling(true));
  }
}
