package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits billing events as structured JSON log lines for downstream billing pipelines.
 *
 * <p>Each call to {@link #bill(ModelUsage, ApiFeatures)} for an eligible request emits:
 *
 * <ul>
 *   <li>a {@code {provider}_{modelType}_tokens} event with {@link ModelUsage#totalTokens()}
 *   <li>a {@code {provider}_egress_bytes} event with {@link ModelUsage#requestBytes()} — bytes sent
 *       from the data plane to the provider
 *   <li>(NVIDIA only) a {@code nvidia_gpu_plane_egress_bytes} event with {@link
 *       ModelUsage#responseBytes()} — bytes coming back from the GPU plane
 * </ul>
 *
 * <p>Eligibility requires all of: {@link ApiFeature#BILLING} is enabled, the {@code billing.events}
 * logger is enabled, and the {@link ModelUsage} is non-null. The region for each event is read from
 * {@link ModelUsage#tenant()} ({@code Tenant.region()}).
 */
@ApplicationScoped
public class Billing {

  private static final Logger BILLING_LOGGER = LoggerFactory.getLogger("billing.events");
  private static final Logger LOGGER = LoggerFactory.getLogger(Billing.class);

  private final String product;
  private final String resourceType;
  private final ObjectWriter objectWriter;

  @Inject
  public Billing(BillingConfig config) {
    Objects.requireNonNull(config, "config must not be null");
    this.product = requireNonBlank(config.product(), "billing.product");
    this.resourceType = requireNonBlank(config.resourceType(), "billing.resource_type");
    this.objectWriter = new ObjectMapper().writer();
  }

  /**
   * Emits billing events for the given aggregated model usage, if the request and configuration
   * allow it. No-op otherwise (feature disabled, logger disabled, null usage).
   */
  public void bill(ModelUsage modelUsage, ApiFeatures apiFeatures) {
    if (!shouldEmit(modelUsage, apiFeatures)) {
      return;
    }
    for (BillingEvent event : buildEvents(modelUsage)) {
      try {
        BILLING_LOGGER.info(objectWriter.writeValueAsString(event));
      } catch (JacksonException e) {
        LOGGER.error("Failed to serialize billing event of type {}", event.eventType(), e);
      }
    }
  }

  /** Whether a billing event should be emitted for the given request. */
  @VisibleForTesting
  boolean shouldEmit(ModelUsage modelUsage, ApiFeatures apiFeatures) {
    return apiFeatures.isFeatureEnabled(ApiFeature.BILLING)
        && BILLING_LOGGER.isInfoEnabled()
        && modelUsage != null;
  }

  /**
   * Builds the list of billing events for one {@link ModelUsage}: a {@code *_tokens} event with
   * {@link ModelUsage#totalTokens()}, a {@code {provider}_egress_bytes} event with {@link
   * ModelUsage#requestBytes()}, and for NVIDIA also a {@code nvidia_gpu_plane_egress_bytes} event
   * with {@link ModelUsage#responseBytes()}.
   *
   * <p>E.g. for an NVIDIA embeddings call with 7 tokens, 512 request bytes and 1024 response bytes,
   * this returns three events of the form:
   *
   * <pre>
   * {"id":"...","timestamp":"...","product":"serverless",
   *  "event_type":"nvidia_embeddings_tokens",
   *  "properties":{"usage":7,"region":"us-west-2",
   *                "resource_type":"serverless_database","resource_id":"&lt;tenant&gt;"}}
   *
   * {"id":"...","timestamp":"...","product":"serverless",
   *  "event_type":"nvidia_egress_bytes",
   *  "properties":{"usage":512,"region":"us-west-2",
   *                "resource_type":"serverless_database","resource_id":"&lt;tenant&gt;"}}
   *
   * {"id":"...","timestamp":"...","product":"serverless",
   *  "event_type":"nvidia_gpu_plane_egress_bytes",
   *  "properties":{"usage":1024,"region":"us-west-2",
   *                "resource_type":"serverless_database","resource_id":"&lt;tenant&gt;"}}
   * </pre>
   */
  @VisibleForTesting
  List<BillingEvent> buildEvents(ModelUsage modelUsage) {
    ModelProvider provider = modelUsage.modelProvider();
    String providerApi = provider.apiName();
    String region = modelUsage.tenant().region();
    String resourceId = modelUsage.tenant().toString();

    var events = new ArrayList<BillingEvent>(3);
    events.add(
        newEvent(
            providerApi + "_" + modelUsage.modelType().billingName() + "_tokens",
            modelUsage.totalTokens(),
            region,
            resourceId));
    events.add(
        newEvent(providerApi + "_egress_bytes", modelUsage.requestBytes(), region, resourceId));
    if (provider == ModelProvider.NVIDIA) {
      events.add(
          newEvent(
              "nvidia_gpu_plane_egress_bytes", modelUsage.responseBytes(), region, resourceId));
    }
    return List.copyOf(events);
  }

  private BillingEvent newEvent(String eventType, long usage, String region, String resourceId) {
    var properties = new BillingEvent.BillingProperties(usage, region, resourceType, resourceId);
    return new BillingEvent(UUID.randomUUID(), Instant.now(), product, eventType, properties);
  }

  private static String requireNonBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be null or blank");
    }
    return value;
  }
}
