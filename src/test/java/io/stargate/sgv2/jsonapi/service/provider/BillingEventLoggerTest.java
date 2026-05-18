package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.junit.jupiter.api.Test;

class BillingEventLoggerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ModelUsage createAstraModelUsage() {
    Tenant tenant = Tenant.create(DatabaseType.ASTRA, "db-uuid-1234");
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        500,
        0,
        0,
        1000L);
  }

  private ModelUsage createCassandraModelUsage() {
    Tenant tenant = Tenant.create(DatabaseType.CASSANDRA, null);
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        500,
        0,
        0,
        1000L);
  }

  @Test
  void buildBillingEventJson_nonAstraTenant_returnsNull() {
    String json = BillingEventLogger.buildBillingEventJson(createCassandraModelUsage());
    assertThat(json).isNull();
  }

  @Test
  void buildBillingEventJson_astraTenant_returnsValidJson() throws Exception {
    String json = BillingEventLogger.buildBillingEventJson(createAstraModelUsage());
    assertThat(json).isNotNull();

    JsonNode node = MAPPER.readTree(json);
    assertThat(node.get("id").asText()).isNotBlank();
    assertThat(node.get("timestamp").asText()).isNotBlank();
    assertThat(node.get("product").asText()).isEqualTo("serverless");
    assertThat(node.get("event_type").asText()).isEqualTo("nvidia_embeddings_tokens");
    assertThat(node.get("properties").get("usage").asInt()).isEqualTo(500);
    assertThat(node.get("properties").get("region").asText()).isEqualTo("us-west-2");
    assertThat(node.get("properties").get("resource_type").asText())
        .isEqualTo("serverless_database");
    assertThat(node.get("properties").get("resource_id").asText()).isEqualTo("db-uuid-1234");
  }
}
