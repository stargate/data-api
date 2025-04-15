package io.stargate.sgv2.jsonapi.config.constants;

/** Defines constants for metric names and tag keys used in the Data API. */
public interface MetricsConstants {
  /** Default value used for tags when the actual value is unknown or unavailable. */
  String UNKNOWN_VALUE = "unknown";

  /** Defines common tag keys used across various metrics. */
  interface MetricTags {
    String TENANT_TAG = "tenant";
    String TABLE_TAG = "table";
    String RERANKING_PROVIDER_TAG = "rerankingProvider";
    String RERANKING_MODEL_TAG = "rerankingModel";
  }

  /** Defines metric names related to HTTP server interactions. */
  interface HttpMetrics {
    String HTTP_SERVER_REQUESTS = "http.server.requests";
  }

  /** Defines metric names related to Reranking operations. */
  interface RerankingMetrics {
    String TENANT_PASSAGE_COUNT_METRIC = "rerank.tenant.passage.count";
    String ALL_PASSAGE_COUNT_METRIC = "rerank.all.passage.count";
    String TENANT_CALL_DURATION_METRIC = "rerank.tenant.call.duration";
    String ALL_CALL_DURATION_METRIC = "rerank.all.call.duration";
  }
}
