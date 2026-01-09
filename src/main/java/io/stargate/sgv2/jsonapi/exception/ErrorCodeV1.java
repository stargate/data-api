package io.stargate.sgv2.jsonapi.exception;

import jakarta.ws.rs.core.Response;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCodeV1 {
  /** Embedding provider service error codes. */
  // !!! 16-Dec-2025, tatu: USED BY EMBEDDING-GATEWAY, DO NOT CONVERT "EMBEDDING_" entries yet
  EMBEDDING_REQUEST_ENCODING_ERROR("Unable to create embedding provider request message"),
  EMBEDDING_RESPONSE_DECODING_ERROR("Unable to parse embedding provider response message"),
  EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED(
      "The Embedding Provider authentication keys not provided"),
  EMBEDDING_PROVIDER_CLIENT_ERROR("The Embedding Provider returned a HTTP client error"),
  EMBEDDING_PROVIDER_SERVER_ERROR("The Embedding Provider returned a HTTP server error"),
  EMBEDDING_PROVIDER_RATE_LIMITED("The Embedding Provider rate limited the request"),
  EMBEDDING_PROVIDER_TIMEOUT("The Embedding Provider timed out"),
  EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE("The Embedding Provider returned an unexpected response"),
  EMBEDDING_PROVIDER_API_KEY_MISSING("The Embedding Provider API key is missing"),

  // CreateCollection error codes:

  COLLECTION_CREATION_ERROR(
      "Collection creation failure (unable to create table). Recommend re-creating the collection"),
  EMBEDDING_SERVICE_NOT_CONFIGURED(
      "Unable to vectorize data, embedding service not configured for the collection "),
  INDEXES_CREATION_FAILED("Index creation failed, check schema"),
  INVALID_INDEXING_DEFINITION("Invalid indexing definition"),
  INVALID_JSONAPI_COLLECTION_SCHEMA("Not a valid json api collection schema"),
  INVALID_VECTORIZE_VALUE_TYPE("$vectorize value needs to be text value"),
  TOO_MANY_COLLECTIONS("Too many collections"),
  TOO_MANY_INDEXES("Too many indexes"),

  RERANKING_FEATURE_NOT_ENABLED("Reranking feature is not enabled"),
  RERANKING_SERVICE_TYPE_UNAVAILABLE("Reranking service unavailable"),
  RERANKING_PROVIDER_UNEXPECTED_RESPONSE("The Reranking Provider returned an unexpected response"),
  RERANKING_PROVIDER_CLIENT_ERROR("The Reranking Provider returned a HTTP client error"),
  RERANKING_PROVIDER_SERVER_ERROR("The Reranking Provider returned a HTTP server error"),
  RERANKING_PROVIDER_RATE_LIMITED("The Reranking Provider rate limited the request"),
  RERANKING_PROVIDER_TIMEOUT("The Reranking Provider timed out"),
  RERANKING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED(
      "The reranking provider authentication key is not provided"),

  INVALID_USAGE_OF_VECTORIZE("`$vectorize` and `$vector` can't be used together"),

  UNSUPPORTED_PROJECTION_PARAM("Unsupported projection parameter"),

  UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),

  UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),

  UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),

  UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),

  UNSUPPORTED_UPDATE_OPERATION_PATH("Unsupported update operation path"),

  UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),

  VECTOR_SEARCH_NOT_SUPPORTED("Vector search is not enabled for the collection"),

  VECTOR_SEARCH_INVALID_FUNCTION_NAME("Invalid vector search function name"),

  VECTOR_SEARCH_UNRECOGNIZED_SOURCE_MODEL_NAME("Unrecognized vector search source model name"),

  VECTOR_SEARCH_TOO_BIG_VALUE("Vector embedding property '$vector' length too big"),
  VECTOR_SIZE_MISMATCH("Length of vector parameter different from declared '$vector' dimension"),

  VECTORIZE_FEATURE_NOT_AVAILABLE("Vectorize feature is not available in the environment"),
  VECTORIZE_SERVICE_NOT_REGISTERED("Vectorize service name provided is not registered : "),
  VECTORIZE_SERVICE_TYPE_UNAVAILABLE("Vectorize service unavailable : "),
  VECTORIZE_INVALID_AUTHENTICATION_TYPE("Invalid vectorize authentication type"),

  VECTORIZE_CREDENTIAL_INVALID("Invalid credential name for vectorize"),
  VECTORIZECONFIG_CHECK_FAIL("Internal server error: VectorizeDefinition check fail"),

  HYBRID_FIELD_CONFLICT(
      "The '$hybrid' field cannot be used with '$lexical', '$vector', or '$vectorize'."),
  HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE("Unsupported JSON value type for '$hybrid' field"),
  HYBRID_FIELD_UNKNOWN_SUBFIELDS("Unrecognized sub-field(s) for '$hybrid' Object"),
  HYBRID_FIELD_UNSUPPORTED_SUBFIELD_VALUE_TYPE(
      "Unsupported JSON value type for '$hybrid' sub-field"),

  // Driver failure codes
  /** Error codes related to driver exceptions. */
  SERVER_CLOSED_CONNECTION("Driver request connection is closed"),
  SERVER_COORDINATOR_FAILURE("Coordinator failed"),
  /** Driver failure other than timeout. */
  SERVER_DRIVER_FAILURE("Driver failed"),
  /** Driver timeout failure. */
  SERVER_DRIVER_TIMEOUT("Driver timeout"),
  /**
   * Error code used for "should never happen" style problems. Suffix part needs to include details
   * of actual issue.
   */
  SERVER_NO_NODE_AVAILABLE("No node was available to execute the query"),
  SERVER_QUERY_CONSISTENCY_FAILURE("Database query consistency failed"),
  SERVER_QUERY_EXECUTION_FAILURE("Database query execution failed"),
  SERVER_READ_FAILED("Database read failed"),
  SERVER_UNHANDLED_ERROR("Server failed"),

  // NOTE: ones used/referenced by `embedding-gateway`, cannot remove:

  INVALID_REQUEST("Request not supported by the data store"),

  EMBEDDING_GATEWAY_ERROR_RATE_LIMIT("Embedding Gateway error rate limit reached for the tenant"),
  EMBEDDING_GATEWAY_PROCESSING_ERROR("Embedding Gateway failed to process request");

  private final String message;

  ErrorCodeV1(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public JsonApiException toApiException(String format, Object... args) {
    return new JsonApiException(this, getErrorMessage(format, args));
  }

  public JsonApiException toApiException(
      Response.Status httpStatus, String format, Object... args) {
    return new JsonApiException(this, getErrorMessage(format, args), null, httpStatus);
  }

  public JsonApiException toApiException(Throwable cause, String format, Object... args) {
    return new JsonApiException(this, getErrorMessage(format, args), cause);
  }

  private String getErrorMessage(String format, Object... args) {
    return message + ": " + String.format(format, args);
  }

  public JsonApiException toApiException() {
    return new JsonApiException(this, message);
  }

  public JsonApiException toApiException(Response.Status httpStatus) {
    return new JsonApiException(this, message, null, httpStatus);
  }
}
