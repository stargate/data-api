package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A billing event emitted as a structured JSON log line for downstream billing pipelines.
 *
 * <p>Each event represents a single billable unit from a user request (e.g. token usage or egress
 * bytes for an embedding or reranking call). Constructed and emitted by {@link Billing}.
 *
 * <p>Example serialized form:
 *
 * <pre>
 * {
 *   "id": "8c0e9b8a-1d3a-4f6b-9c0d-1234567890ab",
 *   "timestamp": "2026-05-20T14:23:11.482Z",
 *   "product": "serverless",
 *   "event_type": "internal_model_total_tokens",
 *   "properties": {
 *     "usage": 7,
 *     "region": "us-west-2",
 *     "resource_type": "serverless_database",
 *     "resource_id": "ab12cd34-5678-90ef-ghij-klmnopqrstuv",
 *     "provider": "nvidia",
 *     "model": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
 *   }
 * }
 * </pre>
 *
 * @param id Unique random-based (UUID v4) identifier for this event.
 * @param timestamp ISO 8601 timestamp of when the event was created.
 * @param product Product identifier, e.g. {@code "serverless"}.
 * @param eventType One of the six {@link BillingEventType} values.
 * @param properties Usage details including the billable amount, region, resource and model
 *     identifiers.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingEvent(
    UUID id,
    @JsonSerialize(using = ToStringSerializer.class) Instant timestamp,
    String product,
    @JsonProperty("event_type") BillingEventType eventType,
    BillingProperties properties)
    implements Recordable {

  public BillingEvent {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(timestamp, "timestamp must not be null");
    Objects.requireNonNull(product, "product must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(properties, "properties must not be null");
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("id", id)
        .append("timestamp", timestamp)
        .append("product", product)
        .append("event_type", eventType.eventName())
        .append("properties", properties);
  }

  /**
   * Billing event properties containing usage metrics and resource / model identifiers.
   *
   * @param usage The billable amount (token count or byte count, depending on event type).
   * @param region Deployment region of the database.
   * @param resourceType Type of the billed resource, e.g. {@code "serverless_database"}.
   * @param resourceId The tenant/database identifier.
   * @param provider The model provider API name, e.g. {@code "nvidia"} or {@code "openai"}.
   * @param model The model name, e.g. {@code "nvidia/llama-3.2-nv-rerankqa-1b-v2"}.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record BillingProperties(
      long usage,
      String region,
      @JsonProperty("resource_type") String resourceType,
      @JsonProperty("resource_id") String resourceId,
      String provider,
      String model)
      implements Recordable {

    public BillingProperties {
      Objects.requireNonNull(region, "region must not be null");
      Objects.requireNonNull(resourceType, "resourceType must not be null");
      Objects.requireNonNull(resourceId, "resourceId must not be null");
      Objects.requireNonNull(provider, "provider must not be null");
      Objects.requireNonNull(model, "model must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("usage", usage)
          .append("region", region)
          .append("resource_type", resourceType)
          .append("resource_id", resourceId)
          .append("provider", provider)
          .append("model", model);
    }
  }
}
