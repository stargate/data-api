package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics.*;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags.*;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.UNKNOWN_VALUE;
import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import io.micrometer.core.instrument.*;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Records metrics related to reranking operations performed within a specific request context.
 *
 * <p>This class separates metrics into two categories based on tagging:
 *
 * <ul>
 *   <li><b>Tenant-Specific Metrics:</b> Tracked per tenant and table (e.g., {@code
 *       rerank.tenant.passage.count}, {@code rerank.tenant.call.duration}). Uses {@code tenant} and
 *       {@code table} tags derived from the {@link RequestContext} and {@link SchemaObject}.
 *   <li><b>Overall Metrics:</b> Aggregated across all tenants, but dimensioned by the reranking
 *       provider and model name (e.g., {@code rerank.all.passage.count}, {@code
 *       rerank.all.call.duration}). Uses {@code provider} and {@code model} tags derived from the
 *       {@link RerankingProvider}.
 * </ul>
 *
 * The duration timers ({@code *.call.duration}) measure the asynchronous execution phase of the
 * reranking call, starting after the initial synchronous setup within the provider and ending when
 * the asynchronous operation completes (successfully or with failure).
 *
 * <p>Note: Configuration of percentiles and histogram settings for timers is handled externally via
 * {@link io.micrometer.core.instrument.config.MeterFilter} beans (see {@link
 * io.stargate.sgv2.jsonapi.api.v1.metrics.MicrometerConfiguration}).
 */
public class RerankingMetrics {
  private final MeterRegistry meterRegistry;
  private final RerankingProvider rerankingProvider;
  private final RequestContext requestContext;
  private final SchemaObject schemaObject;

  /**
   * Constructs a new RerankingMetrics instance for a specific request context.
   *
   * @param meterRegistry The MeterRegistry to use for recording metrics.
   * @param rerankingProvider The RerankingProvider used in the operation, for tagging metrics by
   *     provider/model.
   * @param requestContext The RequestContext for the current request, used for tenant tagging.
   * @param schemaObject The SchemaObject representing the target table, used for table tagging.
   */
  public RerankingMetrics(
      MeterRegistry meterRegistry,
      RerankingProvider rerankingProvider,
      RequestContext requestContext,
      SchemaObject schemaObject) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry cannot be null");
    this.rerankingProvider =
        Objects.requireNonNull(rerankingProvider, "rerankingProvider cannot be null");
    this.requestContext = Objects.requireNonNull(requestContext, "requestContext cannot be null");
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject cannot be null");
  }

  /**
   * Records the number of passages being reranked, updating both the tenant-specific metric and the
   * overall metric.
   *
   * <p>This involves recording the count against two distinct metrics:
   *
   * <ul>
   *   <li>Tenant-specific: {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics#RERANK_TENANT_PASSAGE_COUNT_METRIC}
   *       with tags {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#TENANT_TAG} and {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#TABLE_TAG}.
   *   <li>Overall: {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics#RERANK_ALL_PASSAGE_COUNT_METRIC}
   *       with tags {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#RERANKING_PROVIDER_TAG}
   *       and {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#RERANKING_MODEL_TAG}.
   * </ul>
   *
   * @param passageCount The number of passages.
   */
  public void recordPassageCount(int passageCount) {
    // Record the passage count for the specific tenant and table
    Tags tenantTags =
        new RerankingTagsBuilder()
            .withTenant(requestContext.getTenantId().orElse(UNKNOWN_VALUE))
            .withKeyspace(schemaObject.name().keyspace())
            .withTable(schemaObject.name().table())
            .build();
    meterRegistry.summary(RERANK_TENANT_PASSAGE_COUNT_METRIC, tenantTags).record(passageCount);

    // Record the passage count for all tenants, tagged by provider and model
    Tags allTags =
        new RerankingTagsBuilder()
            .withProvider(classSimpleName(rerankingProvider.getClass()))
            .withModel(rerankingProvider.modelName())
            .build();
    meterRegistry.summary(RERANK_ALL_PASSAGE_COUNT_METRIC, allTags).record(passageCount);
  }

  /**
   * Starts a timer sample to measure the duration of the asynchronous reranking network call phase.
   *
   * <p>The returned sample should be passed to {@link #stopRerankNetworkCallTimer} upon completion
   * of the asynchronous operation.
   *
   * @return A {@link Timer.Sample} instance representing the start time.
   */
  public Timer.Sample startRerankNetworkCallTimer() {
    return Timer.start(meterRegistry);
  }

  /**
   * Stops the timer sample once, calculating the duration, and then records that exact duration
   * against both the tenant-specific and the overall reranking call duration metrics.
   *
   * <p>This ensures the identical duration value is recorded for:
   *
   * <ul>
   *   <li>Tenant-specific: {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics#RERANK_TENANT_CALL_DURATION_METRIC}
   *       with tags {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#TENANT_TAG} and {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#TABLE_TAG}.
   *   <li>Overall: {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics#RERANK_ALL_CALL_DURATION_METRIC}
   *       with tags {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#RERANKING_PROVIDER_TAG}
   *       and {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Tags#RERANKING_MODEL_TAG}.
   * </ul>
   *
   * @param sample The {@link Timer.Sample} started by {@link #startRerankNetworkCallTimer()}. Must
   *     not be null.
   */
  public void stopRerankNetworkCallTimer(Timer.Sample sample) {
    Objects.requireNonNull(sample, "Timer.Sample cannot be null");

    // --- Tenant-Specific Timer ---
    // Build tags for the tenant timer
    Tags tenantTags =
        new RerankingTagsBuilder()
            .withTenant(requestContext.getTenantId().orElse(UNKNOWN_VALUE))
            .withKeyspace(schemaObject.name().keyspace())
            .withTable(schemaObject.name().table())
            .build();
    // Get the tenant timer instance
    Timer tenantTimer = meterRegistry.timer(RERANK_TENANT_CALL_DURATION_METRIC, tenantTags);
    // Stop the sample against the tenant timer. This records the duration AND returns it.
    long durationNanos = sample.stop(tenantTimer);

    // --- Overall Timer ---
    // Build tags for the overall timer
    Tags allTags =
        new RerankingTagsBuilder()
            .withProvider(classSimpleName(rerankingProvider.getClass()))
            .withModel(rerankingProvider.modelName())
            .build();
    // Get the overall timer instance
    Timer allTimer = meterRegistry.timer(RERANK_ALL_CALL_DURATION_METRIC, allTags);
    // Manually record the exact same duration (obtained above) to the overall timer.
    allTimer.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  // --- Static Inner Tag Builder Class ---

  /**
   * Builder for creating {@link Tags} specific to reranking metrics.
   *
   * <p>Allows flexible combination of common reranking-related tags derived from the context
   * provided to the outer {@link RerankingMetrics} instance.
   */
  public static class RerankingTagsBuilder {
    private final List<Tag> currentTags;

    public RerankingTagsBuilder() {
      this.currentTags = new ArrayList<>();
    }

    public RerankingTagsBuilder withTenant(String tenantId) {
      currentTags.add(Tag.of(TENANT_TAG, tenantId));
      return this;
    }

    public RerankingTagsBuilder withKeyspace(String keyspace) {
      currentTags.add(Tag.of(KEYSPACE_TAG, keyspace));
      return this;
    }

    public RerankingTagsBuilder withTable(String table) {
      currentTags.add(Tag.of(TABLE_TAG, table));
      return this;
    }

    public RerankingTagsBuilder withProvider(String provider) {
      // TODO: It's not easy to get the provider name as it is described in the config. So we use
      // the class name to indicate the provider. Need to replace the class name in the future.
      currentTags.add(Tag.of(RERANKING_PROVIDER_TAG, provider));
      return this;
    }

    public RerankingTagsBuilder withModel(String modelName) {
      currentTags.add(Tag.of(RERANKING_MODEL_TAG, modelName));
      return this;
    }

    /**
     * Builds the final {@link Tags} object from the added tags.
     *
     * @return A new {@link Tags} instance containing all tags added via the 'withX' methods.
     */
    public Tags build() {
      // Creates an immutable Tags object from the list
      return Tags.of(currentTags);
    }
  }
}
