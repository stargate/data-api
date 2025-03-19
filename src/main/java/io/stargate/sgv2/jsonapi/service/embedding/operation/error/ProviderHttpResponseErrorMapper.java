package io.stargate.sgv2.jsonapi.service.embedding.operation.error;

import static jakarta.ws.rs.core.Response.Status.Family.CLIENT_ERROR;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import jakarta.ws.rs.core.Response;

/**
 * Maps HTTP responses from external providers to appropriate internal API exceptions. This unified
 * mapper handles error responses from both embedding and reranking providers, translating HTTP
 * status codes into meaningful API exceptions.
 */
public class ProviderHttpResponseErrorMapper {
  /**
   * Provider type enumeration to distinguish between different API provider categories. Used to
   * determine the appropriate error codes for different provider types.
   */
  public enum ProviderType {
    EMBEDDING("Embedding"),
    RERANKING("Reranking");

    private final String apiName;

    ProviderType(String apiName) {
      this.apiName = apiName;
    }

    public String apiName() {
      return apiName;
    }
  }

  /**
   * Maps an HTTP response to a corresponding API exception based on provider type. The method
   * analyzes HTTP status codes and generates appropriate exceptions with detailed error messages.
   *
   * @param providerType the type of provider (EMBEDDING or RERANKING)
   * @param providerName the name of the specific provider
   * @param response the HTTP response from the provider
   * @param message the error message from the provider
   * @return a RuntimeException that corresponds to the specific HTTP response status
   */
  public static RuntimeException mapToAPIException(
      ProviderType providerType, String providerName, Response response, String message) {

    // Format string used for all error messages with specific provider type
    String errorFormat =
        providerType.apiName() + " Provider: %s; HTTP Status: %s; Error Message: %s";

    // Timeout errors: 408 or 504
    if (response.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || response.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return (providerType == ProviderType.EMBEDDING)
          ? ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException(
              errorFormat, providerName, response.getStatus(), message)
          : ErrorCodeV1.RERANKING_PROVIDER_TIMEOUT.toApiException(
              errorFormat, providerName, response.getStatus(), message);
    }

    // Rate limiting: 429
    if (response.getStatus() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return (providerType == ProviderType.EMBEDDING)
          ? ErrorCodeV1.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
              errorFormat, providerName, response.getStatus(), message)
          : ErrorCodeV1.RERANKING_PROVIDER_RATE_LIMITED.toApiException(
              errorFormat, providerName, response.getStatus(), message);
    }

    // Client errors: 4xx (except 429)
    if (response.getStatusInfo().getFamily() == CLIENT_ERROR) {
      return (providerType == ProviderType.EMBEDDING)
          ? ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.toApiException(
              errorFormat, providerName, response.getStatus(), message)
          : ErrorCodeV1.RERANKING_PROVIDER_CLIENT_ERROR.toApiException(
              errorFormat, providerName, response.getStatus(), message);
    }

    // Server errors: 5xx
    if (response.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
      return (providerType == ProviderType.EMBEDDING)
          ? ErrorCodeV1.EMBEDDING_PROVIDER_SERVER_ERROR.toApiException(
              errorFormat, providerName, response.getStatus(), message)
          : ErrorCodeV1.RERANKING_PROVIDER_SERVER_ERROR.toApiException(
              errorFormat, providerName, response.getStatus(), message);
    }

    // // Unexpected errors (should never happen as all status codes are covered above)
    return (providerType == ProviderType.EMBEDDING)
        ? ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
            errorFormat, providerName, response.getStatus(), message)
        : ErrorCodeV1.RERANKING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
            errorFormat, providerName, response.getStatus(), message);
  }
}
