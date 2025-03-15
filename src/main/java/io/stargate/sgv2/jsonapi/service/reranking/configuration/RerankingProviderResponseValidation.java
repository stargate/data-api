package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE;
import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.RERANKING_PROVIDER_UNEXPECTED_RESPONSE;

import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client response filter/interceptor that validates the response from the reranking provider.
 *
 * <p>This filter checks the Content-Type of the response to ensure it is compatible with
 * 'application/json' or 'text/json'. It also verifies the presence of a JSON body in the response.
 *
 * <p>If the response fails the validation, a {@link JsonApiException} is thrown with an appropriate
 * error message.
 */
public class RerankingProviderResponseValidation implements ClientResponseFilter {

  static final MediaType MEDIATYPE_TEXT_JSON = new MediaType("text", "json");

  Logger logger = LoggerFactory.getLogger(RerankingProviderResponseValidation.class);

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
    // If the status is 0, it means something went wrong (maybe a timeout). Directly return and pass
    // the error to the client
    if (responseContext.getStatus() == 0) {
      return;
    }
    // Throw error if there is no response body
    if (!responseContext.hasEntity()) {
      throw RERANKING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
          "No response body from the reranking provider");
    }

    // response should always be JSON; if not, error out, include raw response message for
    // trouble-shooting purposes
    MediaType contentType = responseContext.getMediaType();
    if (contentType == null
        || !(MediaType.APPLICATION_JSON_TYPE.isCompatible(contentType)
            || MEDIATYPE_TEXT_JSON.isCompatible(contentType))) {
      String responseBody = null;
      try {
        // Read the entity stream and convert to string
        responseBody =
            new String(responseContext.getEntityStream().readAllBytes(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        logger.error(
            "Cannot convert the provider's error response to string: {}", e.getMessage(), e);
      }
      throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
          "Expected response Content-Type ('application/json' or 'text/json') from the reranking provider but found '%s'; HTTP Status: %s; The response body is: '%s'.",
          contentType, responseContext.getStatus(), responseBody);
    }
  }
}
