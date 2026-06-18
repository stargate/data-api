package io.stargate.sgv2.jsonapi.config.constants;

public interface HttpConstants {

  /** Data API Authentication token header name. */
  String AUTHENTICATION_TOKEN_HEADER_NAME = "Token";

  /** Data API also supports X-Cassandra-Token for backward compatibility. */
  String DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

  /** Data API Embedding service Authentication token header name. */
  String EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME = "x-embedding-api-key";

  /** Data API Embedding service access id header name. */
  String EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME = "x-embedding-access-id";

  /** Data API Embedding service secret id header name. */
  String EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME = "x-embedding-secret-id";

  /** Data API reranking service Authentication token header name. */
  String RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME = "reranking-api-key";

  /** Bearer prefix for the API key. */
  String BEARER_PREFIX_FOR_API_KEY = "Bearer ";
}
