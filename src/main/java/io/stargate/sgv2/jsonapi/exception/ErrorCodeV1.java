package io.stargate.sgv2.jsonapi.exception;

import jakarta.ws.rs.core.Response;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCodeV1 {
  /** Command error codes. */
  COUNT_READ_FAILED("Unable to count documents"),
  COMMAND_UNKNOWN("Provided command unknown"),
  INVALID_CREATE_COLLECTION_OPTIONS("The provided options are invalid"),
  COMMAND_ACCEPTS_NO_OPTIONS("Command accepts no options"),

  /**
   * Error code used for {@code ConstraintViolationException} failures mapped to {@code
   * JsonApiException}
   */
  COMMAND_FIELD_INVALID("Request invalid"),

  CONCURRENCY_FAILURE("Unable to complete transaction due to concurrent transactions"),
  COLLECTION_NOT_EXIST("Collection does not exist, collection name"),
  DATASET_TOO_BIG("Response data set too big to be sorted, add more filters"),

  DOCUMENT_ALREADY_EXISTS("Document already exists with the given _id"),

  DOCUMENT_UNPARSEABLE("Unable to parse the document"),

  DOCUMENT_REPLACE_DIFFERENT_DOCID(
      "The replace document and document resolved using filter have different _id"),

  /** Embedding provider service error codes. */
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

  FILTER_MULTIPLE_ID_FILTER(
      "Cannot have more than one _id equals filter clause: use $in operator instead"),

  FILTER_FIELDS_LIMIT_VIOLATION("Filter fields size limitation violated"),

  /** note: Only used by EmbeddingGateway */
  INVALID_REQUEST("Request not supported by the data store"),

  INVALID_REQUEST_STRUCTURE_MISMATCH("Request invalid, mismatching JSON structure"),

  INVALID_REQUEST_NOT_JSON("Request invalid, cannot parse as JSON"),

  INVALID_REQUEST_UNKNOWN_FIELD("Request invalid, unrecognized JSON field"),

  INVALID_INDEXING_DEFINITION("Invalid indexing definition"),

  UNINDEXED_FILTER_PATH("Unindexed filter path"),

  UNINDEXED_SORT_PATH("Unindexed sort path"),

  ID_NOT_INDEXED("_id is not indexed"),

  KEYSPACE_DOES_NOT_EXIST("The provided keyspace does not exist"),

  SHRED_BAD_BINARY_VECTOR_VALUE("Bad binary vector value to shred"),

  SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),

  SHRED_BAD_DOCID_EMPTY_STRING("Bad value for '_id' property: empty String not allowed"),

  SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),

  SHRED_BAD_DOCUMENT_VECTOR_TYPE("Bad $vector document type to shred "),

  SHRED_BAD_DOCUMENT_VECTORIZE_TYPE("Bad $vectorize document type to shred "),

  SHRED_BAD_DOCUMENT_LEXICAL_TYPE("Bad type for $lexical content to shred"),

  SHRED_BAD_EJSON_VALUE("Bad JSON Extension value"),

  SHRED_BAD_VECTOR_SIZE("$vector value can't be empty"),

  SHRED_BAD_VECTOR_VALUE("$vector value needs to be array of numbers"),
  SHRED_BAD_VECTORIZE_VALUE("$vectorize search clause needs to be non-blank text value"),

  SHRED_DOC_KEY_NAME_VIOLATION("Document field name invalid"),
  SHRED_DOC_LIMIT_VIOLATION("Document size limitation violated"),

  EXISTING_COLLECTION_DIFFERENT_SETTINGS("Collection already exists"),
  EXISTING_TABLE_NOT_DATA_API_COLLECTION("Existing table not valid Data API Collection"),
  INVALID_VECTORIZE_VALUE_TYPE("$vectorize value needs to be text value"),

  INVALID_FILTER_EXPRESSION("Invalid filter expression"),

  INVALID_JSONAPI_COLLECTION_SCHEMA("Not a valid json api collection schema"),

  RERANKING_SERVICE_TYPE_UNAVAILABLE("Reranking service unavailable : "),
  RERANKING_PROVIDER_UNEXPECTED_RESPONSE("The Reranking Provider returned an unexpected response"),
  RERANKING_PROVIDER_CLIENT_ERROR("The Reranking Provider returned a HTTP client error"),
  RERANKING_PROVIDER_SERVER_ERROR("The Reranking Provider returned a HTTP server error"),
  RERANKING_PROVIDER_TIMEOUT("The Reranking Provider timed out"),
  RERANKING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED(
      "The reranking provider authentication key is not provided"),

  TOO_MANY_COLLECTIONS("Too many collections"),

  TOO_MANY_INDEXES("Too many indexes"),
  INDEXES_CREATION_FAILED("Index creation failed, check schema"),

  UNSUPPORTED_FILTER_DATA_TYPE("Unsupported filter data type"),

  UNSUPPORTED_FILTER_OPERATION("Unsupported filter operator"),

  INVALID_SORT_CLAUSE("Invalid sort clause"),

  INVALID_SORT_CLAUSE_PATH("Invalid sort clause path"),

  INVALID_SORT_CLAUSE_VALUE("Invalid sort clause value"),

  INVALID_USAGE_OF_VECTORIZE("`$vectorize` and `$vector` can't be used together"),

  INVALID_CONTENT_TYPE_HEADER("Invalid Content-Type header"),

  UNSUPPORTED_PROJECTION_DEFINITION("Unsupported projection definition"),

  UNSUPPORTED_PROJECTION_PARAM("Unsupported projection parameter"),

  UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),

  UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),
  EMBEDDING_SERVICE_NOT_CONFIGURED(
      "Unable to vectorize data, embedding service not configured for the collection "),

  UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),

  UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),

  UNSUPPORTED_UPDATE_OPERATION_PATH("Invalid update operation path"),

  UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),

  UNSUPPORTED_UPDATE_FOR_DOC_ID("Cannot use operator with '_id' property"),

  UNSUPPORTED_UPDATE_FOR_VECTOR("Cannot use operator with '$vector' property"),
  UNSUPPORTED_UPDATE_FOR_VECTORIZE("Cannot use operator with '$vectorize' property"),

  VECTOR_SEARCH_NOT_AVAILABLE("Vector search functionality is not available in the backend"),

  VECTOR_SEARCH_USAGE_ERROR("Vector search can't be used with other sort clause"),

  VECTOR_SEARCH_NOT_SUPPORTED("Vector search is not enabled for the collection"),

  VECTOR_SEARCH_INVALID_FUNCTION_NAME("Invalid vector search function name"),

  VECTOR_SEARCH_UNRECOGNIZED_SOURCE_MODEL_NAME("Unrecognized vector search source model name"),

  VECTOR_SEARCH_TOO_BIG_VALUE("Vector embedding property '$vector' length too big"),
  VECTOR_SIZE_MISMATCH("Length of vector parameter different from declared '$vector' dimension"),

  VECTORIZE_MODEL_DEPRECATED("Vectorize model is deprecated"),
  VECTORIZE_FEATURE_NOT_AVAILABLE("Vectorize feature is not available in the environment"),
  VECTORIZE_SERVICE_NOT_REGISTERED("Vectorize service name provided is not registered : "),
  VECTORIZE_SERVICE_TYPE_UNAVAILABLE("Vectorize service unavailable : "),
  VECTORIZE_USAGE_ERROR("Vectorize search can't be used with other sort clause"),
  VECTORIZE_INVALID_AUTHENTICATION_TYPE("Invalid vectorize authentication type"),

  VECTORIZE_CREDENTIAL_INVALID("Invalid credential name for vectorize"),
  VECTORIZECONFIG_CHECK_FAIL("Internal server error: VectorizeDefinition check fail"),

  LEXICAL_NOT_AVAILABLE_FOR_DATABASE("Lexical search is not available on this database"),
  LEXICAL_NOT_ENABLED_FOR_COLLECTION("Lexical search is not enabled for the collection"),

  UNAUTHENTICATED_REQUEST("UNAUTHENTICATED: Invalid token"),
  COLLECTION_CREATION_ERROR(
      "Collection creation failure (unable to create table). Recommend re-creating the collection"),
  OFFLINE_WRITER_SESSION_NOT_FOUND("Offline writer session not found"),
  UNABLE_TO_CREATE_OFFLINE_WRITER_SESSION("Unable to create offline writer session"),
  INVALID_SCHEMA_VERSION(
      "Collection has invalid schema version. Recommend re-creating the collection"),
  INVALID_ID_TYPE("Invalid Id type"),
  INVALID_QUERY("Invalid query"),
  NO_INDEX_ERROR("Faulty collection (missing indexes). Recommend re-creating the collection"),
  MISSING_VECTOR_VALUE("Missing the vector value when building cql"),

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
  SERVER_INTERNAL_ERROR("Server internal error"),
  SERVER_NO_NODE_AVAILABLE("No node was available to execute the query"),
  SERVER_QUERY_CONSISTENCY_FAILURE("Database query consistency failed"),
  SERVER_QUERY_EXECUTION_FAILURE("Database query execution failed"),
  SERVER_READ_FAILED("Database read failed"),
  SERVER_UNHANDLED_ERROR("Server failed"),
  INVALID_PARAMETER_VALIDATION_TYPE("Invalid Parameter Validation Type"),
  SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE("Embedding Gateway is not available"),
  EMBEDDING_GATEWAY_ERROR_RATE_LIMIT("Embedding Gateway error rate limit reached for the tenant"),
  EMBEDDING_GATEWAY_PROCESSING_ERROR("Embedding Gateway failed to process request"),
  // TODO, add this section so we don't have to throw RuntimeExceptions for table work, and it is
  // easy to track, should be improved along with error refactor work
  // Table related
  // TODO: AARON - remove this unused error code, we would not want to return this error to the user
  ERROR_APPLYING_CODEC("Error applying codec"),

  // API Table Error Codes
  TABLE_FEATURE_NOT_ENABLED("API Table feature is not enabled"),

  TABLE_COLUMN_TYPE_NOT_PROVIDED("Column data type not provided as part of definition"),

  TABLE_COLUMN_TYPE_UNSUPPORTED("Unsupported column types"),
  TABLE_COLUMN_UNKNOWN("Column unknown");

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
