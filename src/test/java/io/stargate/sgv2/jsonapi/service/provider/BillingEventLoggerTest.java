package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BillingEventLoggerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TENANT_ID = "db-uuid-1234";
  private static final int TOTAL_TOKENS = 500;

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

  static Stream<Arguments> providerCombinations() {
    return Stream.of(
        Arguments.of(ModelProvider.NVIDIA, ModelType.EMBEDDING, "us-west-2"),
        Arguments.of(ModelProvider.NVIDIA, ModelType.RERANKING, "us-west-2"),
        Arguments.of(ModelProvider.OPENAI, ModelType.EMBEDDING, null));
  }

  @ParameterizedTest
  @MethodSource("providerCombinations")
  void buildBillingEventJson_astraTenant(
      ModelProvider provider, ModelType modelType, String expectedRegion) throws Exception {
    String json = BillingEventLogger.buildBillingEventJson(createModelUsage(provider, modelType));
    assertThat(json).isNotNull();

    JsonNode node = MAPPER.readTree(json);
    assertThat(node.get("id").asText()).isNotBlank();
    assertThat(node.get("timestamp").asText()).isNotBlank();
    assertThat(node.get("product").asText()).isEqualTo("serverless");
    assertThat(node.get("event_type").asText())
        .isEqualTo(provider.apiName() + "_" + modelType.billingName() + "_tokens");

    JsonNode props = node.get("properties");
    assertThat(props.get("usage").asInt()).isEqualTo(TOTAL_TOKENS);
    assertThat(props.get("resource_type").asText()).isEqualTo("serverless_database");
    assertThat(props.get("resource_id").asText()).isEqualTo(TENANT_ID);

    if (expectedRegion != null) {
      assertThat(props.get("region").asText()).isEqualTo(expectedRegion);
    } else {
      assertThat(props.has("region")).isFalse();
    }
  }

  @Test
  void buildBillingEventJson_nonAstraTenant_returnsNull() {
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
            0,
            1000L);
    assertThat(BillingEventLogger.buildBillingEventJson(usage)).isNull();
  }
}
