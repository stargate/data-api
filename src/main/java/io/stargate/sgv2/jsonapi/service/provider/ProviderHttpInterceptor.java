package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingOutputStream;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to track the usage at the http request level to the embedding or reranking provider
 * model service.
 *
 * <p>E.G. When a providerClient registered the interceptor
 * as @RegisterProvider(ProviderHttpInterceptor.class), the interceptor will intercept the http
 * request and response, then add the sent-bytes and received-bytes to the response headers in the
 * response context.
 *
 * <p>Note, if provider already returned content-length in the response header, then the interceptor
 * will reuse it and won't calculate the response size.
 */
public class ProviderHttpInterceptor implements ClientResponseFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProviderHttpInterceptor.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Header name to track the sent_bytes to the provider (extra detailed to avoid collisions) */
  private static final String SENT_BYTES_HEADER = "data-api-model-usage-sent-bytes";

  /**
   * Header name to track the received_bytes from the provider (extra detailed to avoid collisions)
   */
  private static final String RECEIVED_BYTES_HEADER = "data-api-model-usage-received-bytes";

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws IOException {

    long receivedBytes = 0;
    long sentBytes = 0;

    // we may still get called even if the request failed, and we do not get a valid HTTP response,
    // for sanity check that we have the things we need to for processing.
    boolean isValid =
        responseContext != null
            && responseContext.getStatus() > 0
            && responseContext.getHeaders() != null;

    if (!isValid) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "filter() - Invalid responseContext, skipping sent/received bytes tracking. responseContext is null: {}, getStatus: {}, getHeaders: {}",
            responseContext == null,
            responseContext != null ? responseContext.getStatus() : "response null",
            responseContext != null ? responseContext.getHeaders() : "response null");
      }
      return;
    }

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "filter() - requestContext.getUri(): {}, requestContext.getHeaders(): {}",
          requestContext.getUri(),
          requestContext.getStringHeaders());

      LOGGER.trace(
          "filter() - responseContext.getStatus(): {}, responseContext.getHeaders(): {}",
          responseContext.getStatus(),
          responseContext.getHeaders());
    }

    // Parse the request entity stream to measure its size.
    if (requestContext.hasEntity()) {
      try (var cus = new CountingOutputStream(OutputStream.nullOutputStream())) {
        OBJECT_MAPPER.writeValue(cus, requestContext.getEntity());
        sentBytes = cus.getCount();

      } catch (Exception e) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Failed to measure request body size.", e);
        }
      }
    }

    // Use the content-length if present, otherwise parse the response entity stream to measure its
    // size.
    if (responseContext.hasEntity()) {
      receivedBytes = responseContext.getLength();

      // if provider does not return content-length in the response header.
      if (receivedBytes <= 0) {
        // IMPORTANT - need to reset the entity stream so it can be read again, we have not
        // decoded this into objects yet.
        byte[] body = responseContext.getEntityStream().readAllBytes();
        receivedBytes = body.length;
        responseContext.setEntityStream(new ByteArrayInputStream(body));
      }
    }

    responseContext.getHeaders().add(SENT_BYTES_HEADER, String.valueOf(sentBytes));
    responseContext.getHeaders().add(RECEIVED_BYTES_HEADER, String.valueOf(receivedBytes));
  }

  public static int getSentBytes(Response jakartaResponse) {
    return getHeaderInt(jakartaResponse, SENT_BYTES_HEADER);
  }

  public static int getReceivedBytes(Response jakartaResponse) {
    return getHeaderInt(jakartaResponse, RECEIVED_BYTES_HEADER);
  }

  private static int getHeaderInt(Response jakartaResponse, String headerName) {

    if (jakartaResponse == null || jakartaResponse.getHeaders() == null) {
      // log at trace, because this should be detected in filter() method
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "getHeaderInt() - jakartaResponse or headers is null, returning 0 for headerName: {}",
            headerName);
      }
      return 0;
    }

    var headerString = jakartaResponse.getHeaderString(headerName);
    if (headerString != null && !headerString.isBlank()) {
      try {
        return Integer.parseInt(headerString);
      } catch (NumberFormatException e) {
        LOGGER.warn("Failed to parse headerName:{}, headerString:{}", headerName, headerString, e);
      }
    }
    return 0;
  }
}
