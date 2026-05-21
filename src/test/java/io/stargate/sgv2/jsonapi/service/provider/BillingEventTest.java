package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BillingEventTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static BillingEvent.BillingProperties props() {
    return new BillingEvent.BillingProperties(123L, "us-west-2", "serverless_database", "tenant-x");
  }

  @Test
  void canonicalConstructorRejectsNulls() {
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    assertThatThrownBy(() -> new BillingEvent(null, now, "p", "t", props()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BillingEvent(id, null, "p", "t", props()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BillingEvent(id, now, null, "t", props()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BillingEvent(id, now, "p", null, props()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BillingEvent(id, now, "p", "t", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void billingPropertiesRejectsNulls() {
    assertThatThrownBy(() -> new BillingEvent.BillingProperties(1L, null, "rt", "rid"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BillingEvent.BillingProperties(1L, "r", null, "rid"))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new BillingEvent.BillingProperties(1L, "r", "rt", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void serializesIdAsStringAndTimestampAsIso8601() throws Exception {
    UUID id = UUID.randomUUID();
    Instant now = Instant.parse("2026-05-19T00:29:21.506481Z");
    BillingEvent event =
        new BillingEvent(id, now, "serverless", "nvidia_embeddings_tokens", props());

    String json = MAPPER.writeValueAsString(event);
    JsonNode node = MAPPER.readTree(json);

    assertThat(node.get("id").asText()).isEqualTo(id.toString());
    assertThat(node.get("timestamp").asText()).isEqualTo("2026-05-19T00:29:21.506481Z");
    assertThat(node.get("product").asText()).isEqualTo("serverless");
    assertThat(node.get("event_type").asText()).isEqualTo("nvidia_embeddings_tokens");
    assertThat(node.get("properties").get("usage").asLong()).isEqualTo(123L);
    assertThat(node.get("properties").get("region").asText()).isEqualTo("us-west-2");
    assertThat(node.get("properties").get("resource_type").asText())
        .isEqualTo("serverless_database");
    assertThat(node.get("properties").get("resource_id").asText()).isEqualTo("tenant-x");
  }
}
