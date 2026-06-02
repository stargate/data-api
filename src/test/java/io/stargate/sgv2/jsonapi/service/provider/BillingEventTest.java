package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BillingEventTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static BillingEvent.BillingProperties props() {
    return new BillingEvent.BillingProperties(
        123L,
        "us-west-2",
        "serverless_database",
        "tenant-x",
        "nvidia",
        "nvidia/llama-3.2-nv-rerankqa-1b-v2");
  }

  @Test
  void serializesIdAsStringAndTimestampAsIso8601() throws Exception {
    UUID id = UUID.randomUUID();
    Instant now = Instant.parse("2026-05-19T00:29:21.506481Z");
    BillingEvent event =
        new BillingEvent(
            id, now, "serverless", BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS, props());

    String json = MAPPER.writeValueAsString(event);
    JsonNode node = MAPPER.readTree(json);

    assertThat(node.get("id").asText()).isEqualTo(id.toString());
    assertThat(node.get("timestamp").asText()).isEqualTo("2026-05-19T00:29:21.506481Z");
    assertThat(node.get("product").asText()).isEqualTo("serverless");
    assertThat(node.get("event_type").asText())
        .isEqualTo(BillingEventType.INTERNAL_MODEL_TOTAL_TOKENS.eventName());
    assertThat(node.get("properties").get("usage").asLong()).isEqualTo(123L);
    assertThat(node.get("properties").get("region").asText()).isEqualTo("us-west-2");
    assertThat(node.get("properties").get("resource_type").asText())
        .isEqualTo("serverless_database");
    assertThat(node.get("properties").get("resource_id").asText()).isEqualTo("tenant-x");
    assertThat(node.get("properties").get("provider").asText()).isEqualTo("nvidia");
    assertThat(node.get("properties").get("model").asText())
        .isEqualTo("nvidia/llama-3.2-nv-rerankqa-1b-v2");
  }

  @Test
  void serializesAllEventTypesWithExpectedNames() throws Exception {
    for (BillingEventType type : BillingEventType.values()) {
      BillingEvent event =
          new BillingEvent(
              UUID.randomUUID(),
              Instant.parse("2026-05-19T00:29:21.506481Z"),
              "serverless",
              type,
              props());
      String json = MAPPER.writeValueAsString(event);
      JsonNode node = MAPPER.readTree(json);
      assertThat(node.get("event_type").asText()).isEqualTo(type.eventName());
    }
  }
}
