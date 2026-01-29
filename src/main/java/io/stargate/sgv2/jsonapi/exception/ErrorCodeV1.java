package io.stargate.sgv2.jsonapi.exception;

import jakarta.ws.rs.core.Response;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCodeV1 {
  /** Embedding provider service error codes. */
  // !!! 16-Dec-2025, tatu: USED BY EMBEDDING-GATEWAY, DO NOT CONVERT "EMBEDDING_" entries yet

  VECTOR_SEARCH_NOT_SUPPORTED("Vector search is not enabled for the collection"),

  VECTOR_SEARCH_INVALID_FUNCTION_NAME("Invalid vector search function name"),

  VECTOR_SEARCH_TOO_BIG_VALUE("Vector embedding property '$vector' length too big"),

  VECTORIZE_FEATURE_NOT_AVAILABLE("Vectorize feature is not available in the environment"),
  VECTORIZE_SERVICE_NOT_REGISTERED("Vectorize service name provided is not registered : "),
  VECTORIZE_SERVICE_TYPE_UNAVAILABLE("Vectorize service unavailable : "),
  VECTORIZE_INVALID_AUTHENTICATION_TYPE("Invalid vectorize authentication type"),

  VECTORIZE_CREDENTIAL_INVALID("Invalid credential name for vectorize"),

// NOTE: ones used/referenced by `embedding-gateway`, cannot remove:

//  INVALID_REQUEST("Request not supported by the data store"),

//  EMBEDDING_GATEWAY_ERROR_RATE_LIMIT("Embedding Gateway error rate limit reached for the tenant"),
//  EMBEDDING_GATEWAY_REQUEST_PROCESSING_ERROR("Embedding Gateway failed to process request");
;

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
