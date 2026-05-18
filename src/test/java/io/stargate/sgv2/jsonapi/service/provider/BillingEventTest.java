package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BillingEventTest {

  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final String TENANT_ID = "53950f2d-4d4c-4346-a84e-7a07e2ab23f4";
  private static final int TOTAL_TOKENS = 430827772;

  private ModelUsage createModelUsage(ModelProvider provider, ModelType modelType) {
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
        0,
        1000L);
  }

  static Stream<Arguments> providerModelTypeCombinations() {
    return Arrays.stream(ModelProvider.values())
        .flatMap(
            provider -> {
              // Only NVIDIA supports reranking currently
              Stream<ModelType> types =
                  provider == ModelProvider.NVIDIA
                      ? Stream.of(ModelType.EMBEDDING, ModelType.RERANKING)
                      : Stream.of(ModelType.EMBEDDING);
              return types.map(modelType -> Arguments.of(provider, modelType));
            });
  }

  @ParameterizedTest
  @MethodSource("providerModelTypeCombinations")
  void billingEventFields(ModelProvider provider, ModelType modelType) {
    ModelUsage usage = createModelUsage(provider, modelType);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    assertThat(UUID.fromString(event.id())).isNotNull();
    assertThat(Instant.parse(event.timestamp())).isNotNull();
    assertThat(event.product()).isEqualTo(PRODUCT);
    assertThat(event.eventType())
        .isEqualTo(provider.apiName() + "_" + modelType.billingName() + "_tokens");
    assertThat(event.properties().usage()).isEqualTo(TOTAL_TOKENS);
    assertThat(event.properties().region()).isEqualTo(provider.billingRegion());
    assertThat(event.properties().resourceType()).isEqualTo(RESOURCE_TYPE);
    assertThat(event.properties().resourceId()).isEqualTo(TENANT_ID);
  }
}
