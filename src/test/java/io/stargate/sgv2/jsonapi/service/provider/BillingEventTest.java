package io.stargate.sgv2.jsonapi.service.provider;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

  @ParameterizedTest(name = "{0}")
  @EnumSource(BillingEventType.class)
  void serializesEventToExpectedJson(BillingEventType type) throws Exception {
    UUID id = UUID.fromString("c0ffee01-1234-5678-9abc-def012345678");
    Instant timestamp = Instant.parse("2026-05-19T00:29:21.506481Z");
    BillingEvent event = new BillingEvent(id, timestamp, "serverless", type, props());

    String actualJson = MAPPER.writeValueAsString(event);

    String expectedJson =
            """
        {
          "id": "c0ffee01-1234-5678-9abc-def012345678",
          "timestamp": "2026-05-19T00:29:21.506481Z",
          "product": "serverless",
          "event_type": "%s",
          "properties": {
            "usage": 123,
            "region": "us-west-2",
            "resource_type": "serverless_database",
            "resource_id": "tenant-x",
            "provider": "nvidia",
            "model": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
          }
        }
        """
            .formatted(type.eventName());
    assertJsonEquals(expectedJson, actualJson);
  }
}
