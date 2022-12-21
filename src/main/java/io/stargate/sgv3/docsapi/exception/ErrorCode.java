package io.stargate.sgv3.docsapi.exception;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCode {

  /** Command error codes. */
  COMMAND_NOT_IMPLEMENTED("The provided command is not implemented."),

  SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),

  SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),

  SHRED_UNRECOGNIZED_NODE_TYPE("Unrecognized JSON node type in input document");

  private final String message;

  ErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
