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

public class RerankingMetrics {
  private final MeterRegistry meterRegistry;
  private final RerankingProvider rerankingProvider;
  private final RequestContext requestContext;
  private final SchemaObject schemaObject;

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
   * Records the number of passages being reranked for a specific tenant and table. Metric: {@code
   * rerank.tenant.passage.count} Tags: {@code tenant}, {@code table}
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
   * Records the number of passages being reranked across all tenants, tagged by provider and model.
   * Metric: {@code rerank.all.passage.count} Tags: {@code provider}, {@code model}
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
   * Starts a timer sample to measure duration. This sample should be used with both {@link
   * #stopRerankNetworkCallTenantTimer} and {@link #stopRerankNetworkCallAllTimer}.
   *
   * @return A {@link Timer.Sample} instance.
   */
  public Timer.Sample startRerankNetworkCallTimer() {
    return Timer.start(meterRegistry);
  }

  /**
   * Stops the timer and records the duration for the specific tenant and table. Metric: {@code
   * rerank.tenant.call.duration} Tags: {@code tenant}, {@code table} (Expected to be configured for
   * P98 percentile via MeterFilter)
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
   * Stops the timer and records the duration across all tenants, tagged by provider and model.
   * Metric: {@code rerank.all.call.duration} Tags: {@code provider}, {@code model} (Expected to be
   * configured for P80, P95, P98 percentiles via MeterFilter)
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
   * Builder for creating {@link Tags} specific to reranking metrics. Allows flexible combination of
   * common reranking-related tags. Use {@link RerankingMetrics#tagsBuilder()} to get an instance.
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
