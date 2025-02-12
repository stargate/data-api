package io.stargate.sgv2.jsonapi.service.embedding.operation;

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

public class NetworkUsageInterceptor implements ClientResponseFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkUsageInterceptor.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // Jackson object mapper

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws IOException {
    int receivedBytes = 0;
    int sentBytes = 0;
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
    if (responseContext.hasEntity()) {
      receivedBytes = responseContext.getLength();
      if (receivedBytes <= 0) {
        // Read the response entity stream to measure its size
        InputStream inputStream = responseContext.getEntityStream();
        receivedBytes = (int) ByteStreams.copy(inputStream, OutputStream.nullOutputStream());
      }
    }

    responseContext.getHeaders().add("sent-bytes", String.valueOf(sentBytes));
    responseContext.getHeaders().add("received-bytes", String.valueOf(receivedBytes));
  }
}
