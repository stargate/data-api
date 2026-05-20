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
 * @param id Unique random-based (UUID v4) identifier for this event.
 * @param timestamp ISO 8601 timestamp of when the event was created.
 * @param product Product identifier, e.g. {@code "serverless"}.
 * @param eventType Event type, e.g. {@code "nvidia_embeddings_tokens"} or {@code
 *     "nvidia_egress_bytes"}.
 * @param properties Usage details including the billable amount, region, and resource identifiers.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingEvent(
    UUID id,
    @JsonSerialize(using = ToStringSerializer.class) Instant timestamp,
    String product,
    @JsonProperty("event_type") String eventType,
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
        .append("event_type", eventType)
        .append("properties", properties);
  }

  /**
   * Billing event properties containing usage metrics and resource identifiers.
   *
   * @param usage The billable amount (token count or byte count, depending on event type).
   * @param region Deployment region of the database. Required for billing — events are not emitted
   *     when region is not configured.
   * @param resourceType Type of the billed resource, e.g. {@code "serverless_database"}.
   * @param resourceId The tenant/database identifier.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record BillingProperties(
      long usage,
      String region,
      @JsonProperty("resource_type") String resourceType,
      @JsonProperty("resource_id") String resourceId)
      implements Recordable {

    public BillingProperties {
      Objects.requireNonNull(region, "region must not be null");
      Objects.requireNonNull(resourceType, "resourceType must not be null");
      Objects.requireNonNull(resourceId, "resourceId must not be null");
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("usage", usage)
          .append("region", region)
          .append("resource_type", resourceType)
          .append("resource_id", resourceId);
    }
  }
}
