package io.stargate.sgv2.jsonapi.api.request;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.EMBEDDING_PROVIDER_INVALID_RESPONSE;

import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.core.MediaType;

public class EmbeddingProviderHeaderValidation implements ClientResponseFilter {
  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws JsonApiException {
    // Check the Content-Type of the response
    MediaType contentType = responseContext.getMediaType();
    if (contentType == null
        || !(MediaType.APPLICATION_JSON_TYPE.isCompatible(contentType)
            || new MediaType("text", "json").isCompatible(contentType))) {
      throw EMBEDDING_PROVIDER_INVALID_RESPONSE.toApiException(
          "Expected response Content-Type ('application/json' or 'text/json') from the embedding provider but found '%s'",
          contentType);
    }
  }
}
