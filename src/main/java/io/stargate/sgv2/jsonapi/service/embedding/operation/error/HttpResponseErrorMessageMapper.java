package io.stargate.sgv2.jsonapi.service.embedding.operation.error;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.ws.rs.core.Response;

public class HttpResponseErrorMessageMapper {
  /**
   * This method returns default exception based on the http response. Individual provider can
   * provide an override
   *
   * @param response
   * @return
   */
  public static RuntimeException getDefaultException(Response response) {

    // Status code == 408 and 504 for timeout
    if (response.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || response.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return ErrorCode.EMBEDDING_PROVIDER_TIMEOUT.toApiException();
    }

    // Status code == 429
    if (response.getStatus() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return ErrorCode.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
          "Error Code : %s response description : %s",
          response.getStatus(), response.getStatusInfo().getReasonPhrase());
    }

    // Status code in 4XX other than 429
    if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()
        && response.getStatus() < Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      return ErrorCode.EMBEDDING_PROVIDER_INVALID_REQUEST.toApiException(
          "Error Code : %s response description : %s",
          response.getStatus(), response.getStatusInfo().getReasonPhrase());
    }

    // Status code in 5XX
    if (response.getStatus() >= Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      return ErrorCode.EMBEDDING_PROVIDER_SERVER_ERROR.toApiException(
          "Error Code : %s response description : %s",
          response.getStatus(), response.getStatusInfo().getReasonPhrase());
    }

    // All other errors, Should never happen as all errors are covered above
    return ErrorCode.EMBEDDING_PROVIDER_UNAVAILBLE.toApiException();
  }
}
