package io.stargate.sgv2.jsonapi.service.billing;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests for the {@link BillingEventType} */
public class BillingEventTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static BillingEvent.BillingProperties props() {
    return new BillingEvent.BillingProperties(
        123L,
        "us-west-2",
        "serverless_database",
        "tenant-x",
        "nvidia",
        "nvidia/llama-3.2-nv-rerankqa-1b-v2",
        "reranking");
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
            "model": "nvidia/llama-3.2-nv-rerankqa-1b-v2",
            "model_type": "reranking"
          }
        }
        """
            .formatted(type.eventName());
    assertJsonEquals(expectedJson, actualJson);

    // just testing recordTo works, this is used with tracing
    var pprint = PrettyPrintable.pprint(event);
    // below is what it was when test created, white space is ignored in assert
    var expectedPPrint =
            """
            BillingEvent{
              id=c0ffee01-1234-5678-9abc-def012345678,
              timestamp=2026-05-19T00:29:21.506481Z,
              product=serverless,
              event_type=%s,
              properties=BillingProperties{
                  usage=123,
                  region=us-west-2,
                  resource_type=serverless_database,
                  resource_id=tenant-x,
                  provider=nvidia,
                  model=nvidia/llama-3.2-nv-rerankqa-1b-v2,
                  model_type=reranking		}
            }
            """
            .formatted(type.eventName());
    assertThat(pprint)
        .as("Event recordTo includes expected values. type:" + type)
        .isEqualToNormalizingWhitespace(expectedPPrint);
  }
}
