package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import java.io.IOException;
import java.io.InputStream;
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

  // Header name to track the sent_bytes to the provider
  public static final String SENT_BYTES_HEADER = "sent-bytes";
  // Header name to track the received_bytes from the provider
  public static final String RECEIVED_BYTES_HEADER = "received-bytes";

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws IOException {
    int receivedBytes = 0;
    int sentBytes = 0;

    // Parse the request entity stream to measure its size.
    if (requestContext.hasEntity()) {
      try {
        CountingOutputStream cus = new CountingOutputStream(OutputStream.nullOutputStream());
        OBJECT_MAPPER.writeValue(cus, requestContext.getEntity());
        cus.close();
        sentBytes = (int) cus.getCount();
      } catch (Exception e) {
        LOGGER.warn("Failed to measure request body size: " + e.getMessage());
      }
    }

    // Use the content-length if present, otherwise parse the response entity stream to measure its
    // size.
    if (responseContext.hasEntity()) {
      receivedBytes = responseContext.getLength();
      // if provider does not return content-length in the response header.
      if (receivedBytes <= 0) {
        // Read the response entity stream to measure its size
        InputStream inputStream = responseContext.getEntityStream();
        receivedBytes = (int) ByteStreams.copy(inputStream, OutputStream.nullOutputStream());
      }
    }

    responseContext.getHeaders().add(SENT_BYTES_HEADER, String.valueOf(sentBytes));
    responseContext.getHeaders().add(RECEIVED_BYTES_HEADER, String.valueOf(receivedBytes));
  }
}
