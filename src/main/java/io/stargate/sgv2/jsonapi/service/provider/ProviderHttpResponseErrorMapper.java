package io.stargate.sgv2.jsonapi.service.provider;

import static jakarta.ws.rs.core.Response.Status.*;
import static jakarta.ws.rs.core.Response.Status.Family.CLIENT_ERROR;
import static jakarta.ws.rs.core.Response.Status.Family.SERVER_ERROR;

import io.stargate.sgv2.jsonapi.exception.ProviderException;
import jakarta.ws.rs.core.Response;
import java.util.Map;

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
    Map<String, String> errorContext =
        createErrorContext(providerType, providerName, response, message);
    ProviderException.Code errorCode = determineErrorCode(response);

    // Create and return the appropriate exception
    return errorCode.get(errorContext);
  }

  /** Creates a context map with common error information. */
  private static Map<String, String> createErrorContext(
      ProviderType providerType, String providerName, Response response, String message) {
    return Map.of(
        "providerType",
        providerType.apiName(),
        "provider",
        providerName,
        "httpStatus",
        Integer.toString(response.getStatus()),
        "errorMessage",
        message);
  }

  /** Determines the appropriate error code based on the HTTP response. */
  private static ProviderException.Code determineErrorCode(Response response) {
    Response.StatusType status = response.getStatusInfo();

    // Timeout errors: 408 or 504
    if (status == REQUEST_TIMEOUT || status == GATEWAY_TIMEOUT) {
      return ProviderException.Code.TIMEOUT;
    }

    // Rate limiting: 429
    if (status == TOO_MANY_REQUESTS) {
      return ProviderException.Code.TOO_MANY_REQUESTS;
    }

    // Client errors: 4xx (except those already handled)
    if (status.getFamily() == CLIENT_ERROR) {
      return ProviderException.Code.CLIENT_ERROR;
    }

    // Server errors: 5xx
    if (status.getFamily() == SERVER_ERROR) {
      return ProviderException.Code.SERVER_ERROR;
    }

    // Default: unexpected response
    return ProviderException.Code.UNEXPECTED_RESPONSE;
  }
}
