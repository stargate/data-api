package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags.*;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics.*;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.UNKNOWN_VALUE;
import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import io.micrometer.core.instrument.*;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
   * Creates a new tag builder instance initialized with context information. Use this builder to
   * construct the desired set of tags for a metric.
   *
   * @return A new {@link RerankingTagsBuilder} instance.
   */
  private RerankingTagsBuilder tagsBuilder() {
    return new RerankingTagsBuilder();
  }

  /**
   * Records the number of passages being reranked for the specific tenant and table associated with
   * this context.
   *
   * <p>Metric: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics#TENANT_PASSAGE_COUNT_METRIC}
   * <br>
   * Tags: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#TENANT_TAG}, {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#TABLE_TAG}
   *
   * @param passageCount The number of passages.
   */
  public void recordTenantPassageCount(int passageCount) {
    Tags tags =
        tagsBuilder()
            .withTenant(requestContext.getTenantId().orElse(UNKNOWN_VALUE))
            .withTable(schemaObject.name().keyspace() + "." + schemaObject.name().table())
            .build();
    DistributionSummary ds = meterRegistry.summary(TENANT_PASSAGE_COUNT_METRIC, tags);
    ds.record(passageCount);
  }

  /**
   * Records the number of passages being reranked across all tenants, tagged by the provider and
   * model associated with this context.
   *
   * <p>Metric: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics#ALL_PASSAGE_COUNT_METRIC}
   * <br>
   * Tags: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#RERANKING_PROVIDER_TAG},
   * {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#RERANKING_MODEL_TAG}
   *
   * @param passageCount The number of passages.
   */
  public void recordAllPassageCount(int passageCount) {
    Tags tags =
        tagsBuilder()
            .withProvider(classSimpleName(rerankingProvider.getClass()))
            .withModel(rerankingProvider.modelName())
            .build();
    DistributionSummary ds = meterRegistry.summary(ALL_PASSAGE_COUNT_METRIC, tags);
    ds.record(passageCount);
  }

  /**
   * Starts a timer sample to measure the duration of the asynchronous reranking network call phase.
   *
   * <p>This should be called after the synchronous setup in the provider method returns the {@code
   * Uni}. The returned sample should be passed to both {@link #stopRerankNetworkCallTenantTimer}
   * and {@link #stopRerankNetworkCallAllTimer} upon completion of the asynchronous operation.
   *
   * @return A {@link Timer.Sample} instance representing the start time.
   */
  public Timer.Sample startRerankNetworkCallTimer() {
    return Timer.start(meterRegistry);
  }

  /**
   * Stops the timer using the given sample and records the elapsed duration for the specific tenant
   * and table associated with this context. Measures the asynchronous reranking network call phase.
   *
   * <p>Metric: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics#TENANT_CALL_DURATION_METRIC}
   * <br>
   * Tags: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#TENANT_TAG}, {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#TABLE_TAG} <br>
   * (Expected to be configured for P98 percentile via MeterFilter)
   *
   * @param sample The {@link Timer.Sample} started by {@link #startRerankNetworkCallTimer()}. Must
   *     not be null.
   */
  public void stopRerankNetworkCallTenantTimer(Timer.Sample sample) {
    Tags tags =
        tagsBuilder()
            .withTenant(requestContext.getTenantId().orElse(UNKNOWN_VALUE))
            .withTable(schemaObject.name().keyspace() + "." + schemaObject.name().table())
            .build();

    sample.stop(meterRegistry.timer(TENANT_CALL_DURATION_METRIC, tags));
  }

  /**
   * Stops the timer using the given sample and records the elapsed duration across all tenants,
   * tagged by the provider and model associated with this context. Measures the asynchronous
   * reranking network call phase.
   *
   * <p>Metric: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics#ALL_CALL_DURATION_METRIC}
   * <br>
   * Tags: {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#RERANKING_PROVIDER_TAG},
   * {@value
   * io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.MetricTags#RERANKING_MODEL_TAG} <br>
   * (Expected to be configured for P80, P95, P98 percentiles via MeterFilter)
   *
   * @param sample The {@link Timer.Sample} started by {@link #startRerankNetworkCallTimer()}. Must
   *     not be null.
   */
  public void stopRerankNetworkCallAllTimer(Timer.Sample sample) {
    Tags tags =
        tagsBuilder()
            .withProvider(classSimpleName(rerankingProvider.getClass()))
            .withModel(rerankingProvider.modelName())
            .build();

    sample.stop(meterRegistry.timer(ALL_CALL_DURATION_METRIC, tags));
  }

  // --- Static Inner Tag Builder Class ---

  /**
   * Builder for creating {@link Tags} specific to reranking metrics.
   *
   * <p>Allows flexible combination of common reranking-related tags derived from the context
   * provided to the outer {@link RerankingMetrics} instance. Use {@link
   * RerankingMetrics#tagsBuilder()} to get an instance.
   */
  public static class RerankingTagsBuilder {
    private final List<Tag> currentTags;

    /** Private constructor. Use {@link RerankingMetrics#tagsBuilder()} to get an instance. */
    private RerankingTagsBuilder() {
      this.currentTags = new ArrayList<>();
    }

    public RerankingTagsBuilder withTenant(String tenantId) {
      currentTags.add(Tag.of(TENANT_TAG, tenantId));
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
