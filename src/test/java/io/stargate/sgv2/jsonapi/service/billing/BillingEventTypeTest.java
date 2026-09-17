package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.service.billing.BillingEventType.Metric.EGRESS_BYTES;
import static io.stargate.sgv2.jsonapi.service.billing.BillingEventType.Metric.INGRESS_BYTES;
import static io.stargate.sgv2.jsonapi.service.billing.BillingEventType.Metric.TOTAL_TOKENS;
import static io.stargate.sgv2.jsonapi.service.provider.ModelType.EMBEDDING;
import static io.stargate.sgv2.jsonapi.service.provider.ModelType.RERANKING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.sgv2.jsonapi.service.provider.ModelType;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link BillingEventType} */
public class BillingEventTypeTest {

  @ParameterizedTest(name = "{3}")
  @MethodSource("eventTypes")
  public void resolvesEventName(
      ModelType modelType, BillingEventType.Metric metric, boolean internal, String eventName) {
    // downstream pricing matches on these names, do not change them
    assertThat(BillingEventType.of(modelType, metric, internal).eventName()).isEqualTo(eventName);
  }

  private static Stream<Arguments> eventTypes() {
    return Stream.of(
        Arguments.of(EMBEDDING, TOTAL_TOKENS, true, "internal_embedding_total_tokens"),
        Arguments.of(RERANKING, TOTAL_TOKENS, true, "internal_reranking_total_tokens"),
        Arguments.of(EMBEDDING, TOTAL_TOKENS, false, "external_embedding_total_tokens"),
        Arguments.of(RERANKING, TOTAL_TOKENS, false, "external_reranking_total_tokens"),
        Arguments.of(EMBEDDING, EGRESS_BYTES, true, "internal_embedding_egress_bytes"),
        Arguments.of(RERANKING, EGRESS_BYTES, true, "internal_reranking_egress_bytes"),
        Arguments.of(EMBEDDING, EGRESS_BYTES, false, "external_embedding_egress_bytes"),
        Arguments.of(RERANKING, EGRESS_BYTES, false, "external_reranking_egress_bytes"),
        Arguments.of(EMBEDDING, INGRESS_BYTES, true, "internal_embedding_ingress_bytes"),
        Arguments.of(RERANKING, INGRESS_BYTES, true, "internal_reranking_ingress_bytes"),
        Arguments.of(EMBEDDING, INGRESS_BYTES, false, "external_embedding_ingress_bytes"),
        Arguments.of(RERANKING, INGRESS_BYTES, false, "external_reranking_ingress_bytes"));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(BillingEventType.Metric.class)
  public void unspecifiedModelTypeThrows(BillingEventType.Metric metric) {
    assertThatThrownBy(() -> BillingEventType.of(ModelType.MODEL_TYPE_UNSPECIFIED, metric, true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> BillingEventType.of(ModelType.MODEL_TYPE_UNSPECIFIED, metric, false))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
