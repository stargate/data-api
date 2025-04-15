package io.stargate.sgv2.jsonapi.config.constants;

/** Constants for metrics name and tags */
public interface MetricsConstants {
  String UNKNOWN_VALUE = "unknown";

  interface MetricTags {
    String TENANT_TAG = "tenant";
    String TABLE_TAG = "table";
    String RERANKING_PROVIDER_TAG = "rerankingProvider";
    String RERANKING_MODEL_TAG = "rerankingModel";
  }

  interface HttpMetrics {
    String HTTP_SERVER_REQUESTS = "http.server.requests";
  }

  interface RerankingMetrics {
    String TENANT_PASSAGE_COUNT_METRIC = "rerank.tenant.passage.count";
    String ALL_PASSAGE_COUNT_METRIC = "rerank.all.passage.count";
    String TENANT_CALL_DURATION_METRIC = "rerank.tenant.call.duration";
    String ALL_CALL_DURATION_METRIC = "rerank.all.call.duration";
  }
}
