package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BillingEventTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String PRODUCT = "serverless";
  private static final String RESOURCE_TYPE = "serverless_database";
  private static final String TENANT_ID = "53950f2d-4d4c-4346-a84e-7a07e2ab23f4";

  private ModelUsage createModelUsage(
      ModelProvider provider, ModelType modelType, int totalTokens) {
    Tenant tenant = Tenant.create(DatabaseType.ASTRA, TENANT_ID);
    return new ModelUsage(
        provider,
        modelType,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        totalTokens,
        0,
        0,
        1000L);
  }

  @Test
  void fromModelUsage_nvidiaEmbedding() throws Exception {
    ModelUsage usage = createModelUsage(ModelProvider.NVIDIA, ModelType.EMBEDDING, 430827772);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    assertThat(UUID.fromString(event.id())).isNotNull();
    assertThat(Instant.parse(event.timestamp())).isNotNull();
    assertThat(event.product()).isEqualTo(PRODUCT);
    assertThat(event.eventType()).isEqualTo("nvidia_embeddings_tokens");
    assertThat(event.properties().usage()).isEqualTo(430827772);
    assertThat(event.properties().region()).isEqualTo("us-west-2");
    assertThat(event.properties().resourceType()).isEqualTo(RESOURCE_TYPE);
    assertThat(event.properties().resourceId()).isEqualTo(TENANT_ID);
  }

  @Test
  void fromModelUsage_nvidiaReranking() {
    ModelUsage usage = createModelUsage(ModelProvider.NVIDIA, ModelType.RERANKING, 500);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    assertThat(event.eventType()).isEqualTo("nvidia_reranking_tokens");
    assertThat(event.properties().region()).isEqualTo("us-west-2");
  }

  @Test
  void fromModelUsage_openaiEmbedding_noRegion() {
    ModelUsage usage = createModelUsage(ModelProvider.OPENAI, ModelType.EMBEDDING, 1000);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    assertThat(event.eventType()).isEqualTo("openai_embeddings_tokens");
    assertThat(event.properties().region()).isNull();
  }

  @Test
  void jsonSerialization_matchesBillingSchema() throws Exception {
    ModelUsage usage = createModelUsage(ModelProvider.NVIDIA, ModelType.EMBEDDING, 430827772);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    String json = MAPPER.writeValueAsString(event);
    JsonNode node = MAPPER.readTree(json);

    assertThat(node.has("id")).isTrue();
    assertThat(node.has("timestamp")).isTrue();
    assertThat(node.get("product").asText()).isEqualTo(PRODUCT);
    assertThat(node.get("event_type").asText()).isEqualTo("nvidia_embeddings_tokens");

    JsonNode props = node.get("properties");
    assertThat(props.get("usage").asLong()).isEqualTo(430827772);
    assertThat(props.get("region").asText()).isEqualTo("us-west-2");
    assertThat(props.get("resource_type").asText()).isEqualTo(RESOURCE_TYPE);
    assertThat(props.get("resource_id").asText()).isEqualTo(TENANT_ID);
  }

  @Test
  void jsonSerialization_nullRegionOmitted() throws Exception {
    ModelUsage usage = createModelUsage(ModelProvider.OPENAI, ModelType.EMBEDDING, 1000);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    String json = MAPPER.writeValueAsString(event);
    JsonNode props = MAPPER.readTree(json).get("properties");

    assertThat(props.has("region")).isFalse();
  }

  @ParameterizedTest
  @EnumSource(
      value = ModelProvider.class,
      names = {"NVIDIA", "OPENAI", "COHERE", "MISTRAL"})
  void eventType_dynamicForAllProviders(ModelProvider provider) {
    ModelUsage usage = createModelUsage(provider, ModelType.EMBEDDING, 100);
    BillingEvent event = BillingEvent.from(usage, PRODUCT, RESOURCE_TYPE);

    assertThat(event.eventType()).isEqualTo(provider.apiName() + "_embeddings_tokens");
  }
}
