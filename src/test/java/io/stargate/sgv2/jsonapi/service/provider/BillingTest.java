package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BillingTest {

  private static final String TENANT_ID = "db-uuid-1234";
  private static final String REGION = "us-west-2";
  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final String MODEL_NAME = "test-model";
  private static final int TOTAL_TOKENS = 500;
  private static final int REQUEST_BYTES = 256;
  private static final int RESPONSE_BYTES = 12_345;

  private static LoggingBilling newBilling(ApiFeatures apiFeatures) {
    // Optional.empty() means "use all event types" — matches production default behavior.
    return newBilling(apiFeatures, List.of("nvidia"), Optional.empty());
  }

  private static LoggingBilling newBilling(
      ApiFeatures apiFeatures,
      List<String> internalProviders,
      Optional<Set<BillingEventType>> enabledEventTypes) {
    BillingConfig config = mock(BillingConfig.class);
    when(config.product()).thenReturn(PRODUCT);
    when(config.resourceType()).thenReturn(RESOURCE_TYPE);
    when(config.internalModelProviders()).thenReturn(internalProviders);
    when(config.enabledEventTypes()).thenReturn(enabledEventTypes);
    return new LoggingBilling(config, apiFeatures);
  }

  /** Default Billing with BILLING_EVENTS_LOGGING enabled — sufficient for buildEvents tests. */
  private static LoggingBilling newBilling() {
    return newBilling(featuresWithBilling(true));
  }

  private static ApiFeatures featuresWithBilling(boolean enabled) {
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags())
        .thenReturn(Map.of(ApiFeature.BILLING_EVENTS_LOGGING, String.valueOf(enabled)));
    return ApiFeatures.fromConfigAndRequest(config, null);
  }

  private ModelUsage usage(ModelProvider provider, ModelType modelType, Tenant tenant) {
    return new ModelUsage(
        provider,
        modelType,
        MODEL_NAME,
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
  void buildEvents_internalProvider_usesInternalEventTypes() {
    LoggingBilling billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).hasSize(3);

    BillingEvent tokens = events.get(0);
    assertThat(tokens.eventType()).isEqualTo(BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS);
    assertThat(tokens.properties().usage()).isEqualTo(TOTAL_TOKENS);
    assertThat(tokens.properties().region()).isEqualTo(REGION);
    assertThat(tokens.properties().resourceType()).isEqualTo(RESOURCE_TYPE);
    assertThat(tokens.properties().resourceId()).isEqualTo(TENANT_ID);
    assertThat(tokens.properties().provider()).isEqualTo("nvidia");
    assertThat(tokens.properties().model()).isEqualTo(MODEL_NAME);

    BillingEvent egress = events.get(1);
    assertThat(egress.eventType()).isEqualTo(BillingEventType.INTERNAL_MODEL_EGRESS_BYTES);
    assertThat(egress.properties().usage()).isEqualTo(REQUEST_BYTES);

    BillingEvent ingress = events.get(2);
    assertThat(ingress.eventType()).isEqualTo(BillingEventType.INTERNAL_MODEL_INGRESS_BYTES);
    assertThat(ingress.properties().usage()).isEqualTo(RESPONSE_BYTES);
  }

  @Test
  void buildEvents_externalProvider_usesExternalEventTypes() {
    LoggingBilling billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.OPENAI, ModelType.EMBEDDING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).hasSize(3);
    assertThat(events.get(0).eventType()).isEqualTo(BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS);
    assertThat(events.get(0).properties().usage()).isEqualTo(TOTAL_TOKENS);
    assertThat(events.get(0).properties().provider()).isEqualTo("openai");
    assertThat(events.get(0).properties().model()).isEqualTo(MODEL_NAME);

    assertThat(events.get(1).eventType()).isEqualTo(BillingEventType.EXTERNAL_MODEL_EGRESS_BYTES);
    assertThat(events.get(1).properties().usage()).isEqualTo(REQUEST_BYTES);

    assertThat(events.get(2).eventType()).isEqualTo(BillingEventType.EXTERNAL_MODEL_INGRESS_BYTES);
    assertThat(events.get(2).properties().usage()).isEqualTo(RESPONSE_BYTES);
  }

  @Test
  void buildEvents_modelTypeDoesNotChangeEventType() {
    // Reranking and embedding produce the same event types — the dimension is in event_type, the
    // distinction lives in properties.model.
    LoggingBilling billing = newBilling();
    ModelUsage rerank = usage(ModelProvider.NVIDIA, ModelType.RERANKING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(rerank);

    assertThat(events)
        .extracting(BillingEvent::eventType)
        .containsExactly(
            BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
            BillingEventType.INTERNAL_MODEL_EGRESS_BYTES,
            BillingEventType.INTERNAL_MODEL_INGRESS_BYTES);
  }

  @Test
  void buildEvents_filtersDisabledEventTypes() {
    // Only enable total_tokens variants — egress / ingress events should be dropped.
    LoggingBilling billing =
        newBilling(
            featuresWithBilling(true),
            List.of("nvidia"),
            Optional.of(
                EnumSet.of(
                    BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
                    BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS)));

    ModelUsage internal = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));
    ModelUsage external = usage(ModelProvider.OPENAI, ModelType.EMBEDDING, astraTenant(REGION));

    assertThat(billing.buildEvents(internal))
        .singleElement()
        .extracting(BillingEvent::eventType)
        .isEqualTo(BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS);
    assertThat(billing.buildEvents(external))
        .singleElement()
        .extracting(BillingEvent::eventType)
        .isEqualTo(BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS);
  }

  @Test
  void buildEvents_emptyEnabledEventTypes_emitsNothing() {
    // Optional.of(empty set) explicitly disables all billing events — distinct from
    // Optional.empty() which means "use the default = all enabled".
    LoggingBilling billing =
        newBilling(featuresWithBilling(true), List.of("nvidia"), Optional.of(Set.of()));
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    assertThat(billing.buildEvents(modelUsage)).isEmpty();
  }

  @Test
  void buildEvents_emptyInternalProviders_allEventsAreExternal() {
    // With no providers listed as internal, even NVIDIA usage is classified external.
    LoggingBilling billing = newBilling(featuresWithBilling(true), List.of(), Optional.empty());
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events)
        .extracting(BillingEvent::eventType)
        .containsExactly(
            BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS,
            BillingEventType.EXTERNAL_MODEL_EGRESS_BYTES,
            BillingEventType.EXTERNAL_MODEL_INGRESS_BYTES);
  }

  @Test
  void buildEvents_cassandraTenant_usesCassandraDefaultRegion() {
    LoggingBilling billing = newBilling();
    Tenant cassandraTenant = Tenant.create(DatabaseType.CASSANDRA, null);
    ModelUsage modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, cassandraTenant);

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events)
        .allMatch(e -> Tenant.CASSANDRA_REGION_DEFAULT.equals(e.properties().region()));
  }

  @Test
  void buildEvents_astraTenantWithoutRegion_usesUnknownRegion() {
    LoggingBilling billing = newBilling();
    ModelUsage modelUsage = usage(ModelProvider.OPENAI, ModelType.EMBEDDING, astraTenant(null));

    List<BillingEvent> events = billing.buildEvents(modelUsage);

    assertThat(events).allMatch(e -> Tenant.UNKNOWN_REGION.equals(e.properties().region()));
  }

  @Test
  void shouldEmit_trueWhenFeatureEnabled() {
    LoggingBilling billing = newBilling(featuresWithBilling(true));

    assertThat(billing.shouldEmit()).isTrue();
  }

  @Test
  void shouldEmit_falseWhenFeatureDisabled() {
    LoggingBilling billing = newBilling(featuresWithBilling(false));

    assertThat(billing.shouldEmit()).isFalse();
  }

  @Test
  void emitEvent_nullUsageThrows() {
    // Callers must guarantee usage data exists before invoking. A null modelUsage would silently
    // mask a calling-side bug, so emitEvent rejects it loudly.
    LoggingBilling billing = newBilling(featuresWithBilling(true));

    assertThatThrownBy(() -> billing.emitEvent(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("modelUsage");
  }

  @Test
  void emitEvent_featureDisabledIsNoOp() {
    // BILLING_EVENTS_LOGGING disabled → emitEvent does nothing, even with valid usage.
    newBilling(featuresWithBilling(false))
        .emitEvent(usage(ModelProvider.NVIDIA, ModelType.EMBEDDING, astraTenant(REGION)));
  }

  /**
   * If BILLING_EVENTS_LOGGING is enabled at startup config, a request header MUST NOT be able to
   * turn it off. The config value is authoritative; headers are only consulted when config leaves
   * the flag unset.
   */
  @Test
  void shouldEmit_configEnabledIsNotOverriddenByHeader() {
    // startup config: billing = true
    FeaturesConfig config = mock(FeaturesConfig.class);
    when(config.flags()).thenReturn(Map.of(ApiFeature.BILLING_EVENTS_LOGGING, "true"));

    // request header tries to disable it
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add(ApiFeature.BILLING_EVENTS_LOGGING.httpHeaderName(), "false");
    var headerAccess = new RequestContext.HttpHeaderAccess(headers);

    ApiFeatures apiFeatures = ApiFeatures.fromConfigAndRequest(config, headerAccess);

    LoggingBilling billing = newBilling(apiFeatures);

    assertThat(apiFeatures.isFeatureEnabled(ApiFeature.BILLING_EVENTS_LOGGING))
        .as("config=true must win over header=false")
        .isTrue();
    assertThat(billing.shouldEmit()).isTrue();
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
    headers.add(ApiFeature.BILLING_EVENTS_LOGGING.httpHeaderName(), "true");
    var headerAccess = new RequestContext.HttpHeaderAccess(headers);

    ApiFeatures apiFeatures = ApiFeatures.fromConfigAndRequest(config, headerAccess);

    LoggingBilling billing = newBilling(apiFeatures);

    assertThat(billing.shouldEmit()).isTrue();
  }
}
