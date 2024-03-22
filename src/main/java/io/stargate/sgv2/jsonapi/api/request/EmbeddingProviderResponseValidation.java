package io.stargate.sgv2.jsonapi.api.request;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.EMBEDDING_PROVIDER_INVALID_RESPONSE;

import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.core.MediaType;

/**
 * A client response filter/interceptor that validates the response from the embedding provider.
 *
 * <p>This filter checks the Content-Type of the response to ensure it is compatible with
 * 'application/json' or 'text/json'. It also verifies the presence of a JSON body in the response.
 *
 * <p>If the response fails the validation, a {@link JsonApiException} is thrown with an appropriate
 * error message.
 */
public class EmbeddingProviderResponseValidation implements ClientResponseFilter {

  static final MediaType MEDIATYPE_TEXT_JSON = new MediaType("text", "json");

  /**
   * Filters the client response by validating the Content-Type and JSON body.
   *
   * @param requestContext the client request context
   * @param responseContext the client response context
   * @throws JsonApiException if the response fails the validation
   */
  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws JsonApiException {
    // Check the Content-Type of the response
    MediaType contentType = responseContext.getMediaType();
    if (contentType == null
        || !(MediaType.APPLICATION_JSON_TYPE.isCompatible(contentType)
            || MEDIATYPE_TEXT_JSON.isCompatible(contentType))) {
      throw EMBEDDING_PROVIDER_INVALID_RESPONSE.toApiException(
          "Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found '%s'",
          contentType);
    }

    // Throw error if there is no response body
    if (!responseContext.hasEntity()) {
      throw EMBEDDING_PROVIDER_INVALID_RESPONSE.toApiException(
          "No JSON body from the embedding provider");
    }
  }
}
