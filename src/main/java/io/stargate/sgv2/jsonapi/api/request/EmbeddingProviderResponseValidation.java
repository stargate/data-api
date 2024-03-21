package io.stargate.sgv2.jsonapi.api.request;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.EMBEDDING_PROVIDER_INVALID_RESPONSE;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class EmbeddingProviderResponseValidation implements ClientResponseFilter {
  @Inject ObjectMapper objectMapper;

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

    // Validate if the response body is a valid JSON
    if (!responseContext.hasEntity()) {
      throw EMBEDDING_PROVIDER_INVALID_RESPONSE.toApiException(
          "No JSON body from the embedding provider");
    }
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] responseData;
    try {
      InputStream originalStream = responseContext.getEntityStream();
      originalStream.transferTo(buffer);
      responseData = buffer.toByteArray();
      // Reset the entity stream so that it can be read again later
      responseContext.setEntityStream(new ByteArrayInputStream(responseData));
      objectMapper.readTree(responseData);
    } catch (IOException e) {
      throw EMBEDDING_PROVIDER_INVALID_RESPONSE.toApiException(
          "Invalid JSON response from the embedding provider");
    }
  }
}
