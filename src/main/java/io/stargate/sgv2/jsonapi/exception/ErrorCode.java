package io.stargate.sgv2.jsonapi.exception;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCode {

  /** Command error codes. */
  COMMAND_NOT_IMPLEMENTED("The provided command is not implemented."),

  DOCUMENT_ALREADY_EXISTS("Document already exists with the id"),

  DOCUMENT_UNPARSEABLE("Unable to parse the document"),

  FILTER_UNRESOLVABLE("Unable to resolve the filter"),

  SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),

  SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),

  SHRED_BAD_DOCID_EMPTY_STRING("Bad value for '_id' property: empty String not allowed"),

  SHRED_INTERNAL_NO_PATH("Internal: path being built does not point to a property or element"),

  SHRED_NO_MD5("MD5 Hash algorithm not available"),

  SHRED_UNRECOGNIZED_NODE_TYPE("Unrecognized JSON node type in input document"),

  UNSUPPORTED_FILTER_DATA_TYPE("Unsupported filter data type"),

  UNSUPPORTED_FILTER_OPERATION("Unsupported filter operator"),

  UNSUPPORTED_OPERATION("Unsupported operation class"),

  UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),

  UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),

  UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),

  UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),

  UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),

  UNSUPPORTED_UPDATE_FOR_DOC_ID("Cannot use operator with '_id' field");

  private final String message;

  ErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
