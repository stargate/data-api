package io.stargate.sgv2.jsonapi.metrics;

/** Defines constants for metric names and tag keys used in the Data API. */
public interface MetricsConstants {
  /** Default value used for tags when the actual value is unknown or unavailable. */
  String UNKNOWN_VALUE = "unknown";

  /** Defines common tag keys used across various metrics. */
  interface MetricTags {
    String KEYSPACE_TAG = "keyspace";
    String RERANKING_PROVIDER_TAG = "reranking.provider";
    String RERANKING_MODEL_TAG = "reranking.model";
    String SESSION_TAG = "session";
    String TENANT_TAG = "tenant";
    String TABLE_TAG = "table";
  }

  /** Defines metric names that used in the DataAPI */
  interface MetricNames {
    String HTTP_SERVER_REQUESTS = "http.server.requests";
    String RERANK_ALL_CALL_DURATION_METRIC = "rerank.all.call.duration";
    String RERANK_ALL_INFLIGHT_CALLS_METRIC = "rerank.all.inflight.calls";
    String RERANK_ALL_PASSAGE_COUNT_METRIC = "rerank.all.passage.count";
    String RERANK_ALL_QUEUE_DEPTH_METRIC = "rerank.all.queue.depth";
    String RERANK_ALL_QUEUE_REJECTED_METRIC = "rerank.all.queue.rejected.count";
    String RERANK_ALL_QUEUE_WAIT_DURATION_METRIC = "rerank.all.queue.wait.duration";
    String RERANK_TENANT_CALL_DURATION_METRIC = "rerank.tenant.call.duration";
    String RERANK_TENANT_PASSAGE_COUNT_METRIC = "rerank.tenant.passage.count";
    String VECTORIZE_CALL_DURATION_METRIC = "vectorize.call.duration";
  }
}
