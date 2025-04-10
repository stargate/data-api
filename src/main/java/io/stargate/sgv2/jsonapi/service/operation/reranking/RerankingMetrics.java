package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import io.micrometer.core.instrument.*;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RerankingMetrics {
  final String RERANKING_NUMBER_OF_PASSAGE = "reranking.passage.count";
  final String RERANKING_CALL_DURATION_METRICS = "reranking.call.duration";
  final String RERANKING_PROVIDER_METRICS_TAG = "reranking.provider";
  final String RERANKING_PROVIDER_MODEL_METRICS_TAG = "reranking.model";
  final String TENANT_TAG = "tenant";
  final String UNKNOWN_VALUE = "unknown";
  private final MeterRegistry meterRegistry;
  private final RerankingProvider rerankingProvider;
  private final RequestContext requestContext;

  public RerankingMetrics(
      MeterRegistry meterRegistry,
      RerankingProvider rerankingProvider,
      RequestContext requestContext) {
    this.meterRegistry = meterRegistry;
    this.rerankingProvider = rerankingProvider;
    this.requestContext = requestContext;
  }

  void recordTotalNumberOfPassages(List<String> passages) {
    Objects.requireNonNull(passages);
    DistributionSummary ds = meterRegistry.summary(RERANKING_NUMBER_OF_PASSAGE, getCustomTags());
    ds.record(passages.size());
  }

  Timer.Sample startTimer() {
    return Timer.start(meterRegistry);
  }

  void stopTimer(Timer.Sample sample) {
    sample.stop(meterRegistry.timer(RERANKING_CALL_DURATION_METRICS, getCustomTags()));
  }

  private Tags getCustomTags() {
    Tag tenantTag = Tag.of(TENANT_TAG, requestContext.getTenantId().orElse(UNKNOWN_VALUE));
    Tag rerankingProviderTag =
        Tag.of(RERANKING_PROVIDER_METRICS_TAG, classSimpleName(rerankingProvider.getClass()));
    Tag rerankingModelTag =
        Tag.of(
            RERANKING_PROVIDER_MODEL_METRICS_TAG,
            Optional.ofNullable(rerankingProvider.modelName()).orElse(UNKNOWN_VALUE));
    return Tags.of(tenantTag, rerankingProviderTag, rerankingModelTag);
  }
}
