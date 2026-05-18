package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.time.Instant;
import java.util.UUID;

/**
 * A billing event emitted as a structured JSON log line for downstream billing pipelines.
 *
 * <p>Each event represents the aggregated token usage from a single user request to a model
 * provider (embedding or reranking). Created via {@link #from(ModelUsage, String, String)} after
 * batch aggregation.
 *
 * @param id Unique UUID v4 identifier for this event.
 * @param timestamp ISO 8601 timestamp of when the event was created.
 * @param product Product identifier, e.g. {@code "serverless"}.
 * @param eventType Event type in the format {@code {provider}_{modelType}_tokens}, e.g. {@code
 *     "nvidia_embeddings_tokens"}.
 * @param properties Usage details including token count, region, and resource identifiers.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BillingEvent(
    String id,
    String timestamp,
    String product,
    @JsonProperty("event_type") String eventType,
    BillingProperties properties)
    implements Recordable {

  /**
   * Billing event properties containing usage metrics and resource identifiers.
   *
   * @param usage Total token count from the provider response.
   * @param region Deployment region for the provider, or {@code null} if not applicable (omitted
   *     from JSON).
   * @param resourceType Type of the billed resource, e.g. {@code "serverless_database"}.
   * @param resourceId The tenant/database ID used to infer the billing tenant.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record BillingProperties(
      long usage,
      String region,
      @JsonProperty("resource_type") String resourceType,
      @JsonProperty("resource_id") String resourceId)
      implements Recordable {

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("usage", usage)
          .append("region", region)
          .append("resource_type", resourceType)
          .append("resource_id", resourceId);
    }
  }

  /**
   * Creates a billing event from an aggregated {@link ModelUsage}.
   *
   * @param modelUsage Aggregated model usage from a completed provider request.
   * @param product Product identifier from configuration.
   * @param resourceType Resource type from configuration.
   */
  public static BillingEvent from(ModelUsage modelUsage, String product, String resourceType) {
    String eventType =
        modelUsage.modelProvider().apiName()
            + "_"
            + modelUsage.modelType().billingName()
            + "_tokens";

    var properties =
        new BillingProperties(
            modelUsage.totalTokens(),
            modelUsage.modelProvider().billingRegion(),
            resourceType,
            modelUsage.tenant().toString());

    return new BillingEvent(
        UUID.randomUUID().toString(), Instant.now().toString(), product, eventType, properties);
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
}
