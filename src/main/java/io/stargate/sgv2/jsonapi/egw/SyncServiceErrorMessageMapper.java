package io.stargate.sgv2.jsonapi.egw;

import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncServiceErrorMessageMapper {
  private static final Logger logger = LoggerFactory.getLogger(SyncServiceErrorMessageMapper.class);

  /**
   * This method returns default exception based on the http response.
   *
   * @param response
   * @return
   */
  public static RuntimeException getDefaultException(Response response) {

    // Status code == 408 and 504 for timeout
    if (response.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || response.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {

      return EmbeddingProviderException.Code.EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE
          .get("errorMessage", "Sync service timed out");
    }

    // Status code in 5XX
    if (response.getStatus() >= Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      logger.error(
          String.format(
              "5xx response failure, mapped to EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE: error code: %s, response description: %s",
              response.getStatus(), response.getStatusInfo().getReasonPhrase()));

      return EmbeddingProviderException.Code.EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE
          .get(
              "errorMessage",
              "Sync service has internal server error. Error Code: %s; response description: %s"
                  .formatted(response.getStatus(), response.getStatusInfo().getReasonPhrase()));
    }

    // All other errors, Should never happen as all errors are covered above
    logger.error(
        String.format(
            "Unrecognized response failure, mapped to EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE: error code: %s, response description: %s",
            response.getStatus(), response.getStatusInfo().getReasonPhrase()));

    return EmbeddingProviderException.Code.EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE.get(
        "errorMessage",
        "Sync service is not available. Error Code : %s response description : %s"
            .formatted(response.getStatus(), response.getStatusInfo().getReasonPhrase()));
  }
}
