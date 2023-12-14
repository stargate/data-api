package io.stargate.sgv2.jsonapi.exception;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCode {
  /** Command error codes. */
  COMMAND_NOT_IMPLEMENTED("The provided command is not implemented."),

  COMMAND_ACCEPTS_NO_OPTIONS("Command accepts no options"),

  CONCURRENCY_FAILURE("Unable to complete transaction due to concurrent transactions"),
  COLLECTION_NOT_EXIST("Collection does not exist, collection name: "),
  DATASET_TOO_BIG("Response data set too big to be sorted, add more filters"),

  DOCUMENT_ALREADY_EXISTS("Document already exists with the given _id"),

  DOCUMENT_UNPARSEABLE("Unable to parse the document"),

  DOCUMENT_REPLACE_DIFFERENT_DOCID(
      "The replace document and document resolved using filter have different _id"),

  FILTER_UNRESOLVABLE("Unable to resolve the filter"),

  FILTER_MULTIPLE_ID_FILTER(
      "Should only have one _id filter, document id cannot be restricted by more than one relation if it includes an Equal"),

  FILTER_FIELDS_LIMIT_VIOLATION("Filter fields size limitation violated"),

  INVALID_REQUST("Request not supported by the data store"),

  NAMESPACE_DOES_NOT_EXIST("The provided namespace does not exist."),

  SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),

  SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),

  SHRED_BAD_DOCUMENT_VECTOR_TYPE("Bad $vector document type to shred "),

  SHRED_BAD_DOCUMENT_VECTORIZE_TYPE("Bad $vectorize document type to shred "),

  SHRED_BAD_DOCID_EMPTY_STRING("Bad value for '_id' property: empty String not allowed"),

  SHRED_INTERNAL_NO_PATH("Internal: path being built does not point to a property or element"),

  SHRED_NO_MD5("MD5 Hash algorithm not available"),

  SHRED_UNRECOGNIZED_NODE_TYPE("Unrecognized JSON node type in input document"),

  SHRED_DOC_LIMIT_VIOLATION("Document size limitation violated"),

  SHRED_DOC_KEY_NAME_VIOLATION("Document key name constraints violated"),

  SHRED_BAD_EJSON_VALUE("Bad EJSON value"),

  SHRED_BAD_VECTOR_SIZE("$vector field can't be empty"),

  SHRED_BAD_VECTOR_VALUE("$vector search needs to be array of numbers"),
  SHRED_BAD_VECTORIZE_VALUE("$vectorize search needs to be text value"),

  INVALID_FILTER_EXPRESSION("Invalid filter expression"),

  INVALID_COLLECTION_NAME("Invalid collection name "),

  INVALID_JSONAPI_COLLECTION_SCHEMA("Not a valid json api collection schema: "),

  TOO_MANY_COLLECTIONS("Too many collections"),

  TOO_MANY_INDEXES("Too many indexes"),

  UNSUPPORTED_FILTER_DATA_TYPE("Unsupported filter data type"),

  UNSUPPORTED_FILTER_OPERATION("Unsupported filter operator"),

  INVALID_SORT_CLAUSE_PATH("Invalid sort clause path"),

  INVALID_SORT_CLAUSE_VALUE(
      "Sort ordering value can only be `1` for ascending or `-1` for descending."),

  INVALID_USAGE_OF_VECTORIZE("`$vectorize` and `$vector` can't be used together."),

  UNSUPPORTED_OPERATION("Unsupported operation class"),

  UNSUPPORTED_PROJECTION_PARAM("Unsupported projection parameter"),

  UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),

  UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),

  UNSUPPORTED_COMMAND_EMBEDDING_SERVICE(
      "Unsupported command `createEmbeddingService` since application is configured for property based embedding"),

  UNAVAILABLE_EMBEDDING_SERVICE("Unable to vectorize data, embedding service not available"),

  UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),

  UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),

  UNSUPPORTED_UPDATE_OPERATION_PATH("Invalid update operation path"),

  UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),

  UNSUPPORTED_UPDATE_FOR_DOC_ID("Cannot use operator with '_id' field"),

  UNSUPPORTED_UPDATE_FOR_VECTOR("Cannot use operator with '$vector' field"),
  UNSUPPORTED_UPDATE_FOR_VECTORIZE("Cannot use operator with '$vectorize' field"),

  VECTOR_SEARCH_NOT_AVAILABLE("Vector search functionality is not available in the backend"),

  VECTOR_SEARCH_USAGE_ERROR("Vector search can't be used with other sort clause"),

  VECTOR_SEARCH_NOT_SUPPORTED("Vector search is not enabled for the collection "),

  VECTOR_SEARCH_INVALID_FUNCTION_NAME("Invalid vector search function name: "),

  VECTOR_SEARCH_FIELD_TOO_BIG("Vector embedding field '$vector' length too big"),
  VECTORIZE_SERVICE_NOT_REGISTERED("Vectorize service name provided is not registered : "),

  VECTORIZE_SERVICE_TYPE_NOT_ENABLED("Vectorize service type not enabled : "),
  VECTORIZE_SERVICE_TYPE_UNSUPPORTED("Vectorize service type unsupported : "),

  VECTORIZE_SERVICE_TYPE_UNAVAILABLE("Vectorize service unavailable : "),
  VECTORIZE_USAGE_ERROR("Vectorize search can't be used with other sort clause"),

  VECTORIZECONFIG_CHECK_FAIL("Internal server error: VectorizeConfig check fail");

  private final String message;

  ErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
