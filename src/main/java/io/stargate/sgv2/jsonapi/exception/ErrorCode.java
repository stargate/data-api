package io.stargate.sgv2.jsonapi.exception;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCode {

  /** Command error codes. */
  COMMAND_NOT_IMPLEMENTED("The provided command is not implemented."),

  COMMAND_ACCEPTS_NO_OPTIONS("Command accepts no options"),

  CONCURRENCY_FAILURE("Unable to complete transaction due to concurrent transactions"),

  DATASET_TOO_BIG("Response data set too big to be sorted, add more filters"),

  DOCUMENT_ALREADY_EXISTS("Document already exists with the given _id"),

  DOCUMENT_UNPARSEABLE("Unable to parse the document"),

  DOCUMENT_REPLACE_DIFFERENT_DOCID(
      "The replace document and document resolved using filter have different _id"),

  FILTER_UNRESOLVABLE("Unable to resolve the filter"),

  NAMESPACE_DOES_NOT_EXIST("The provided namespace does not exist."),

  SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),

  SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),

  SHRED_BAD_DOCID_EMPTY_STRING("Bad value for '_id' property: empty String not allowed"),

  SHRED_INTERNAL_NO_PATH("Internal: path being built does not point to a property or element"),

  SHRED_NO_MD5("MD5 Hash algorithm not available"),

  SHRED_UNRECOGNIZED_NODE_TYPE("Unrecognized JSON node type in input document"),

  SHRED_DOC_LIMIT_VIOLATION("Document size limitation violated"),

  SHRED_DOC_KEY_NAME_VIOLATION("Document key name constraints violated"),

  SHRED_BAD_EJSON_VALUE("Bad EJSON value"),

  INVALID_FILTER_EXPRESSION("Invalid filter expression"),

  UNSUPPORTED_FILTER_DATA_TYPE("Unsupported filter data type"),

  UNSUPPORTED_FILTER_OPERATION("Unsupported filter operator"),

  UNSUPPORTED_OPERATION("Unsupported operation class"),

  UNSUPPORTED_PROJECTION_PARAM("Unsupported projection parameter"),

  UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),

  UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),

  UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),

  UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),

  UNSUPPORTED_UPDATE_OPERATION_PATH("Invalid update operation path"),

  UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),

  UNSUPPORTED_UPDATE_FOR_DOC_ID("Cannot use operator with '_id' field"),

  VECTOR_SEARCH_NOT_AVAILABLE("Vector search functionality is not available in the backend");

  private final String message;

  ErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
