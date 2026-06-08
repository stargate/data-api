package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.Test;

class DefaultBillingTest {

  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final String MODEL_NAME = "test-model";
  private static final int TOTAL_TOKENS = 500;
  private static final int REQUEST_BYTES = 256;
  private static final int RESPONSE_BYTES = 12_345;

  // Placeholders for the random id / runtime timestamp on BillingEvent — the record's compact
  // constructor rejects null, but these fields are ignored in the comparison via
  // usingRecursiveComparison().ignoringFields("id", "timestamp").
  private static final UUID PLACEHOLDER_ID = new UUID(0L, 0L);
  private static final Instant PLACEHOLDER_TIMESTAMP = Instant.EPOCH;

  private final TestConstants testConstants = new TestConstants();

  // ============================================================
  // buildEvents
  // ============================================================

  @Test
  void buildEventsInternalProviderProducesAllInternalEvents() {
    var billing = newBilling();
    var modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING);

    var events = billing.buildEvents(modelUsage);

    assertThat(events)
        .usingRecursiveComparison()
        .ignoringFields("id", "timestamp")
        .isEqualTo(
            List.of(
                expectedEvent(
                    BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
                    TOTAL_TOKENS,
                    ModelProvider.NVIDIA.apiName()),
                expectedEvent(
                    BillingEventType.INTERNAL_MODEL_EGRESS_BYTES,
                    REQUEST_BYTES,
                    ModelProvider.NVIDIA.apiName()),
                expectedEvent(
                    BillingEventType.INTERNAL_MODEL_INGRESS_BYTES,
                    RESPONSE_BYTES,
                    ModelProvider.NVIDIA.apiName())));
  }

  @Test
  void buildEventsExternalProviderProducesAllExternalEvents() {
    var billing = newBilling();
    var modelUsage = usage(ModelProvider.OPENAI, ModelType.EMBEDDING);

    var events = billing.buildEvents(modelUsage);

    assertThat(events)
        .usingRecursiveComparison()
        .ignoringFields("id", "timestamp")
        .isEqualTo(
            List.of(
                expectedEvent(
                    BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS,
                    TOTAL_TOKENS,
                    ModelProvider.OPENAI.apiName()),
                expectedEvent(
                    BillingEventType.EXTERNAL_MODEL_EGRESS_BYTES,
                    REQUEST_BYTES,
                    ModelProvider.OPENAI.apiName()),
                expectedEvent(
                    BillingEventType.EXTERNAL_MODEL_INGRESS_BYTES,
                    RESPONSE_BYTES,
                    ModelProvider.OPENAI.apiName())));
  }

  @Test
  void buildEventsModelTypeDoesNotChangeEventType() {
    // Reranking and embedding produce the same event types — the metric is in event_type, the
    // distinction lives in properties.model.
    var billing = newBilling();
    var rerank = usage(ModelProvider.NVIDIA, ModelType.RERANKING);

    var events = billing.buildEvents(rerank);

    assertThat(events)
        .extracting(BillingEvent::eventType)
        .containsExactly(
            BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
            BillingEventType.INTERNAL_MODEL_EGRESS_BYTES,
            BillingEventType.INTERNAL_MODEL_INGRESS_BYTES);
  }

  @Test
  void buildEventsFiltersDisabledEventTypes() {
    // Only enable total_tokens variants — egress / ingress events should be dropped.
    var billing =
        newBilling(
            List.of(ModelProvider.NVIDIA.apiName()),
            Optional.of(
                EnumSet.of(
                    BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
                    BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS)));

    var internal = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING);
    var external = usage(ModelProvider.OPENAI, ModelType.EMBEDDING);

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
  void buildEventsEmptyEnabledEventTypesEmitsNothing() {
    // Optional.of(empty set) explicitly disables all billing events — distinct from
    // Optional.empty() which means "use the default = all enabled".
    var billing = newBilling(List.of(ModelProvider.NVIDIA.apiName()), Optional.of(Set.of()));
    var modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING);

    assertThat(billing.buildEvents(modelUsage)).isEmpty();
  }

  @Test
  void buildEventsEmptyInternalProvidersAllEventsAreExternal() {
    // With no providers listed as internal, even NVIDIA usage is classified external.
    var billing = newBilling(List.of(), Optional.empty());
    var modelUsage = usage(ModelProvider.NVIDIA, ModelType.EMBEDDING);

    var events = billing.buildEvents(modelUsage);

    assertThat(events)
        .extracting(BillingEvent::eventType)
        .containsExactly(
            BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS,
            BillingEventType.EXTERNAL_MODEL_EGRESS_BYTES,
            BillingEventType.EXTERNAL_MODEL_INGRESS_BYTES);
  }

  // ============================================================
  // emitEvent
  // ============================================================

  @Test
  void emitEventNullUsageThrows() {
    // Callers must guarantee usage data exists before invoking. A null modelUsage would silently
    // mask a calling-side bug, so emitEvent rejects it loudly.
    var billing = newBilling();

    assertThatThrownBy(() -> billing.emitEvent(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("modelUsage");
  }

  /**
   * Verifies events are actually written to the {@code billing.events} logger (slf4j → JBoss
   * Logging → JUL under Quarkus, so we capture via JUL Handler). One {@link ModelUsage} → three log
   * records, one per billable metric.
   */
  @Test
  void emitEventWritesOneRecordPerBillableMetricToBillingLogger() {
    var records = new ArrayList<LogRecord>();
    var handler =
        new Handler() {
          @Override
          public void publish(LogRecord r) {
            records.add(r);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    var julLogger = java.util.logging.Logger.getLogger("billing.events");
    julLogger.addHandler(handler);
    try {
      var billing = newBilling();
      billing.emitEvent(usage(ModelProvider.NVIDIA, ModelType.EMBEDDING));

      assertThat(records).hasSize(3);
      assertThat(records)
          .extracting(LogRecord::getMessage)
          .allMatch(msg -> msg.contains("\"product\":\"" + PRODUCT + "\""))
          .anyMatch(msg -> msg.contains("\"event_type\":\"internal_model_total_tokens\""))
          .anyMatch(msg -> msg.contains("\"event_type\":\"internal_model_egress_bytes\""))
          .anyMatch(msg -> msg.contains("\"event_type\":\"internal_model_ingress_bytes\""));
    } finally {
      julLogger.removeHandler(handler);
    }
  }

  // ============================================================
  // Helpers
  // ============================================================

  /** Default config — used by buildEvents tests that don't care about filtering. */
  private static DefaultBilling newBilling() {
    return newBilling(List.of("nvidia"), Optional.empty());
  }

  private static DefaultBilling newBilling(
      List<String> internalProviders, Optional<Set<BillingEventType>> enabledEventTypes) {
    var config = mock(BillingConfig.class);
    when(config.product()).thenReturn(PRODUCT);
    when(config.resourceType()).thenReturn(RESOURCE_TYPE);
    when(config.internalModelProviders()).thenReturn(internalProviders);
    when(config.enabledEventTypes()).thenReturn(enabledEventTypes);
    return new DefaultBilling(config);
  }

  private ModelUsage usage(ModelProvider provider, ModelType modelType) {
    return new ModelUsage(
        provider,
        modelType,
        MODEL_NAME,
        testConstants.TENANT,
        ModelInputType.INDEX,
        100,
        TOTAL_TOKENS,
        REQUEST_BYTES,
        RESPONSE_BYTES,
        1000L);
  }

  /**
   * Builds an expected {@link BillingEvent}. Reads tenant id and region from {@link
   * TestConstants#TENANT} so this stays consistent with what {@link DefaultBilling#buildEvents}
   * reads off the {@link ModelUsage}.
   */
  private BillingEvent expectedEvent(BillingEventType eventType, long usage, String providerName) {
    var properties =
        new BillingEvent.BillingProperties(
            usage,
            testConstants.TENANT.region(),
            RESOURCE_TYPE,
            testConstants.TENANT.toString(),
            providerName,
            MODEL_NAME);
    return new BillingEvent(PLACEHOLDER_ID, PLACEHOLDER_TIMESTAMP, PRODUCT, eventType, properties);
  }
}
