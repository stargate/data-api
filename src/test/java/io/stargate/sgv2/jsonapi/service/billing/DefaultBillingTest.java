package io.stargate.sgv2.jsonapi.service.billing;

import static java.util.logging.Logger.getLogger;
import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelType;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link DefaultBilling} */
public class DefaultBillingTest {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final String MODEL_NAME = "test-model";
  private static final int TOTAL_TOKENS = 500;
  private static final int REQUEST_BYTES = 256;
  private static final int RESPONSE_BYTES = 12_345;
  private static final List<String> INTERNAL_PROVIDERS = List.of(ModelProvider.NVIDIA.apiName());

  // Placeholders for the random id / runtime timestamp on BillingEvent — the record's compact
  // constructor rejects null, but these fields are ignored in the comparison via
  // usingRecursiveComparison().ignoringFields("id", "timestamp").
  private static final UUID PLACEHOLDER_ID = new UUID(0L, 0L);
  private static final Instant PLACEHOLDER_TIMESTAMP = Instant.EPOCH;

  // ============================================================
  // buildEvents
  // ============================================================

  @ParameterizedTest(name = "{0} + {1}")
  @MethodSource("providerAndModelTypeMatrix")
  void buildEventsProducesExpectedEventsForEveryProviderAndModelType(
      ModelProvider provider, ModelType modelType) {
    // every (provider, modelType) combination, internal/external and model_type are independent

    var billing = newBilling(INTERNAL_PROVIDERS, Optional.empty());
    var modelUsage = usage(provider, modelType);
    var events = billing.buildEvents(modelUsage);

    var isInternal = INTERNAL_PROVIDERS.contains(provider.apiName());
    var totalTokensType =
        isInternal
            ? BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS
            : BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS;
    var egressType =
        isInternal
            ? BillingEventType.INTERNAL_MODEL_EGRESS_BYTES
            : BillingEventType.EXTERNAL_MODEL_EGRESS_BYTES;
    var ingressType =
        isInternal
            ? BillingEventType.INTERNAL_MODEL_INGRESS_BYTES
            : BillingEventType.EXTERNAL_MODEL_INGRESS_BYTES;

    assertThat(events)
        .usingRecursiveComparison()
        .ignoringFields("id", "timestamp")
        .isEqualTo(
            List.of(
                expectedEvent(totalTokensType, TOTAL_TOKENS, provider.apiName(), modelType),
                expectedEvent(egressType, REQUEST_BYTES, provider.apiName(), modelType),
                expectedEvent(ingressType, RESPONSE_BYTES, provider.apiName(), modelType)));
  }

  private static Stream<Arguments> providerAndModelTypeMatrix() {
    return Arrays.stream(ModelProvider.values())
        .flatMap(
            provider ->
                Arrays.stream(ModelType.values())
                    .map(modelType -> Arguments.of(provider, modelType)));
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("modelTypeWireValues")
  void buildEventsSetsModelTypeOnEveryEvent(ModelType modelType, String expectedWireValue) {
    // downstream pricing matches on these values, do not change them
    var billing = newBilling();

    var events = billing.buildEvents(usage(ModelProvider.NVIDIA, modelType));

    assertThat(events)
        .hasSize(3)
        .extracting(event -> event.properties().modelType())
        .containsOnly(expectedWireValue);
  }

  private static Stream<Arguments> modelTypeWireValues() {
    return Stream.of(
        Arguments.of(ModelType.EMBEDDING, "embedding"),
        Arguments.of(ModelType.RERANKING, "reranking"),
        Arguments.of(ModelType.MODEL_TYPE_UNSPECIFIED, "unspecified"));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(ModelProvider.class)
  void buildEventsFiltersDisabledEventTypes(ModelProvider provider) {
    // Only TOTAL_TOKENS variants are enabled — egress / ingress events should be dropped, even
    // though the model usage carries values for all three metrics.
    var billing =
        newBilling(
            INTERNAL_PROVIDERS,
            Optional.of(
                EnumSet.of(
                    BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
                    BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS)));
    var modelUsage = usage(provider, ModelType.EMBEDDING);

    var events = billing.buildEvents(modelUsage);

    var isInternal = INTERNAL_PROVIDERS.contains(provider.apiName());
    var totalTokensType =
        isInternal
            ? BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS
            : BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS;

    assertThat(events)
        .usingRecursiveComparison()
        .ignoringFields("id", "timestamp")
        .isEqualTo(
            List.of(
                expectedEvent(
                    totalTokensType, TOTAL_TOKENS, provider.apiName(), ModelType.EMBEDDING)));
  }

  @Test
  void buildEventsEmptyEnabledEventTypesEmitsNothing() {
    // Optional.of(empty set) explicitly disables all billing events — distinct from
    // Optional.empty() which means "use the default = all enabled".
    var billing = newBilling(INTERNAL_PROVIDERS, Optional.of(Set.of()));
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
        .usingRecursiveComparison()
        .ignoringFields("id", "timestamp")
        .isEqualTo(
            List.of(
                expectedEvent(
                    BillingEventType.EXTERNAL_MODEL_TOTAL_TOKENS,
                    TOTAL_TOKENS,
                    ModelProvider.NVIDIA.apiName(),
                    ModelType.EMBEDDING),
                expectedEvent(
                    BillingEventType.EXTERNAL_MODEL_EGRESS_BYTES,
                    REQUEST_BYTES,
                    ModelProvider.NVIDIA.apiName(),
                    ModelType.EMBEDDING),
                expectedEvent(
                    BillingEventType.EXTERNAL_MODEL_INGRESS_BYTES,
                    RESPONSE_BYTES,
                    ModelProvider.NVIDIA.apiName(),
                    ModelType.EMBEDDING)));
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

    var julLogger = getLogger(DefaultBilling.BILLING_LOGGER_NAME);
    julLogger.addHandler(handler);

    try {
      var billing = newBilling();
      billing.emitEvent(usage(ModelProvider.NVIDIA, ModelType.EMBEDDING));

      assertThat(records).hasSize(3);
      var providerName = ModelProvider.NVIDIA.apiName();
      assertJsonEquals(
          expectedEventJson(
              BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS,
              TOTAL_TOKENS,
              providerName,
              "embedding"),
          records.get(0).getMessage());
      assertJsonEquals(
          expectedEventJson(
              BillingEventType.INTERNAL_MODEL_EGRESS_BYTES,
              REQUEST_BYTES,
              providerName,
              "embedding"),
          records.get(1).getMessage());
      assertJsonEquals(
          expectedEventJson(
              BillingEventType.INTERNAL_MODEL_INGRESS_BYTES,
              RESPONSE_BYTES,
              providerName,
              "embedding"),
          records.get(2).getMessage());
    } finally {
      julLogger.removeHandler(handler);
    }
  }

  // ============================================================
  // Helpers
  // ============================================================

  /** Default config — used by buildEvents tests that don't care about filtering. */
  private static DefaultBilling newBilling() {
    return newBilling(INTERNAL_PROVIDERS, Optional.empty());
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
        TEST_CONSTANTS.TENANT,
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
  private BillingEvent expectedEvent(
      BillingEventType eventType, long usage, String providerName, ModelType modelType) {
    var properties =
        new BillingEvent.BillingProperties(
            usage,
            TEST_CONSTANTS.TENANT.region(),
            RESOURCE_TYPE,
            TEST_CONSTANTS.TENANT.toString(),
            providerName,
            MODEL_NAME,
            modelType.apiName());
    return new BillingEvent(PLACEHOLDER_ID, PLACEHOLDER_TIMESTAMP, PRODUCT, eventType, properties);
  }

  /**
   * Expected serialized JSON for one billing event. {@code id} and {@code timestamp} are matched
   * loosely as any string (UUID / Instant generated at emit time); every other field is pinned
   * exactly so we catch regressions in serialization or property mapping.
   */
  private String expectedEventJson(
      BillingEventType eventType, long usage, String providerName, String modelType) {
    return
        """
        {
          "id": "${json-unit.any-string}",
          "timestamp": "${json-unit.any-string}",
          "product": "%s",
          "event_type": "%s",
          "properties": {
            "usage": %d,
            "region": "%s",
            "resource_type": "%s",
            "resource_id": "%s",
            "provider": "%s",
            "model": "%s",
            "model_type": "%s"
          }
        }
        """
        .formatted(
            PRODUCT,
            eventType.eventName(),
            usage,
            TEST_CONSTANTS.TENANT.region(),
            RESOURCE_TYPE,
            TEST_CONSTANTS.TENANT.toString(),
            providerName,
            MODEL_NAME,
            modelType);
  }
}
