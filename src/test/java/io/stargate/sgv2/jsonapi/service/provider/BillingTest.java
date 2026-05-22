package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.junit.jupiter.api.Test;

class BillingTest {

  private static final String TENANT_ID = "db-uuid-1234";
  private static final String REGION = "us-west-2";
  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final int TOTAL_TOKENS = 500;
  private static final int REQUEST_BYTES = 256;
  private static final int RESPONSE_BYTES = 12_345;

  private Billing newBilling() {
    BillingConfig config = mock(BillingConfig.class);
    when(config.product()).thenReturn(PRODUCT);
    when(config.resourceType()).thenReturn(RESOURCE_TYPE);
    return new Billing(config);
  }

  private ApiFeatures featuresWithBilling(boolean enabled) {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of(ApiFeature.BILLING, String.valueOf(enabled)));
    return ApiFeatures.fromConfigAndRequest(config, null);
  }

  private ModelUsage usage(ModelProvider provider, ModelType modelType, Tenant tenant) {
    return new ModelUsage(
        provider,
        modelType,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        TOTAL_TOKENS,
        REQUEST_BYTES,
        RESPONSE_BYTES,
        1000L);
  }

  private Tenant astraTenant(String region) {
    return Tenant.create(DatabaseType.ASTRA, TENANT_ID, region);
  }

  @Test
  void buildEvents_nvidiaEmbedding_emitsThreeEvents() {
    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).hasSize(3);

    BillingEvent tokens = events.get(0);
    assertThat(tokens.eventType()).isEqualTo("nvidia_embeddings_tokens");
    assertThat(tokens.properties().usage()).isEqualTo(TOTAL_TOKENS);
    assertThat(tokens.properties().region()).isEqualTo(REGION);
    assertThat(tokens.properties().resourceType()).isEqualTo(RESOURCE_TYPE);
    assertThat(tokens.properties().resourceId()).isEqualTo(TENANT_ID);

    BillingEvent egress = events.get(1);
    assertThat(egress.eventType()).isEqualTo("nvidia_egress_bytes");
    assertThat(egress.properties().usage()).isEqualTo(REQUEST_BYTES);
    assertThat(egress.properties().region()).isEqualTo(REGION);

    BillingEvent gpuPlane = events.get(2);
    assertThat(gpuPlane.eventType()).isEqualTo("nvidia_gpu_plane_egress_bytes");
    assertThat(gpuPlane.properties().usage()).isEqualTo(RESPONSE_BYTES);
    assertThat(gpuPlane.properties().region()).isEqualTo(REGION);
  }

  @Test
  void buildEvents_nvidiaReranking_emitsThreeEvents() {
    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.RERANKING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).hasSize(3);
    assertThat(events.get(0).eventType()).isEqualTo("nvidia_reranking_tokens");
    assertThat(events.get(1).eventType()).isEqualTo("nvidia_egress_bytes");
    assertThat(events.get(2).eventType()).isEqualTo("nvidia_gpu_plane_egress_bytes");
  }

  @Test
  void buildEvents_nonNvidia_emitsOnlyTwoEvents() {
    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.OPENAI, ModelType.EMBEDDING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).hasSize(2);
    assertThat(events.get(0).eventType()).isEqualTo("openai_embeddings_tokens");
    assertThat(events.get(1).eventType()).isEqualTo("openai_egress_bytes");
    assertThat(events.get(1).properties().usage()).isEqualTo(REQUEST_BYTES);
  }

  @Test
  void buildEvents_cassandraTenant_usesCassandraDefaultRegion() {
    Billing billing = newBilling();
    Tenant cassandraTenant = Tenant.create(DatabaseType.CASSANDRA, null);
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, cassandraTenant);

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events)
        .allMatch(e -> Tenant.CASSANDRA_REGION_DEFAULT.equals(e.properties().region()));
  }

  @Test
  void buildEvents_astraTenantWithoutRegion_usesUnknownRegion() {
    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.OPENAI, ModelType.EMBEDDING, astraTenant(null));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).allMatch(e -> Tenant.UNKNOWN_REGION.equals(e.properties().region()));
  }

  @Test
  void shouldEmit_trueWhenFeatureEnabledAndUsageNonNull() {
    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    assertThat(billing.shouldEmit(modelUsage, featuresWithBilling(true))).isTrue();
  }

  @Test
  void shouldEmit_falseForNullUsage() {
    assertThat(newBilling().shouldEmit(null, featuresWithBilling(true))).isFalse();
  }

  @Test
  void shouldEmit_falseWhenFeatureDisabled() {
    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    assertThat(billing.shouldEmit(modelUsage, featuresWithBilling(false))).isFalse();
  }

  @Test
  void bill_isNoOpWhenGatesFail() {
    Billing billing = newBilling();
    billing.bill(null, featuresWithBilling(true));
    billing.bill(
        usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION)),
        featuresWithBilling(false));
  }

  /**
   * If BILLING is enabled at startup config, a request header MUST NOT be able to turn it off. The
   * config value is authoritative; headers are only consulted when config leaves the flag unset.
   */
  @Test
  void shouldEmit_configEnabledIsNotOverriddenByHeader() {
    // startup config: billing = true
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of(ApiFeature.BILLING, "true"));

    // request header tries to disable it
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING.httpHeaderName(), "false");
    var headerAccess = new RequestContext.HttpHeaderAccess(headers);

    ApiFeatures apiFeatures = ApiFeatures.fromConfigAndRequest(config, headerAccess);

    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    assertThat(apiFeatures.isFeatureEnabled(ApiFeature.BILLING))
        .as("config=true must win over header=false")
        .isTrue();
    assertThat(billing.shouldEmit(modelUsage, apiFeatures)).isTrue();
  }

  /**
   * Conversely, if config doesn't set the flag (left blank / not present), a request header CAN
   * enable it. This proves the header path is alive when config is silent — otherwise the test
   * above would pass trivially.
   */
  @Test
  void shouldEmit_headerEnablesWhenConfigUnset() {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of());

    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING.httpHeaderName(), "true");
    var headerAccess = new RequestContext.HttpHeaderAccess(headers);

    ApiFeatures apiFeatures = ApiFeatures.fromConfigAndRequest(config, headerAccess);

    Billing billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    assertThat(billing.shouldEmit(modelUsage, apiFeatures)).isTrue();
  }
}
