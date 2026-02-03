package io.stargate.sgv2.jsonapi.exception;

import jakarta.ws.rs.core.Response;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCodeV1 {
  /** Embedding provider service error codes. */

  // REMOVE:
  VECTOR_SEARCH_TOO_BIG_VALUE("Vector embedding property '$vector' length too big"),
  // REMOVE:
  VECTORIZE_FEATURE_NOT_AVAILABLE("Vectorize feature is not available in the environment"),

  // REMOVE:
  VECTORIZE_CREDENTIAL_INVALID("Invalid credential name for vectorize"),
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
