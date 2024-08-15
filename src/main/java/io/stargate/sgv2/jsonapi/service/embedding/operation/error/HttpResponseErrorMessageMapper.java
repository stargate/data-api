package io.stargate.sgv2.jsonapi.service.embedding.operation.error;

import static jakarta.ws.rs.core.Response.Status.Family.CLIENT_ERROR;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.ws.rs.core.Response;

public class HttpResponseErrorMessageMapper {
  /**
   * Maps an HTTP response to a corresponding API exception. Individual providers can override this
   * method to provide custom exception handling.
   *
   * @param providerName the name of the provider
   * @param response the HTTP response
   * @param message the error message from provider
   * @return a JsonApiException that corresponds to the specific HTTP response status
   */
  public static RuntimeException mapToAPIException(
      String providerName, Response response, String message) {
    // Status code == 408 and 504 for timeout
    if (response.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || response.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return ErrorCode.EMBEDDING_PROVIDER_TIMEOUT.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          providerName, response.getStatus(), message);
    }

    // Status code == 429
    if (response.getStatus() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return ErrorCode.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          providerName, response.getStatus(), message);
    }

    // Status code in 4XX other than 429
    if (response.getStatusInfo().getFamily() == CLIENT_ERROR) {
      return ErrorCode.EMBEDDING_PROVIDER_CLIENT_ERROR.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          providerName, response.getStatus(), message);
    }

    // Status code in 5XX
    if (response.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
      return ErrorCode.EMBEDDING_PROVIDER_SERVER_ERROR.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          providerName, response.getStatus(), message);
    }

    // All other errors, Should never happen as all errors are covered above
    return ErrorCode.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
        "Provider: %s; HTTP Status: %s; Error Message: %s",
        providerName, response.getStatus(), message);
  }
}
