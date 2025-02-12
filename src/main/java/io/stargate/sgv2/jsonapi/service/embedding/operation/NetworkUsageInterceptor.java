package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

public class NetworkUsageInterceptor implements ClientResponseFilter {

  private static final Logger LOGGER = Logger.getLogger(NetworkUsageInterceptor.class.getName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // Jackson object mapper

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws IOException {
    int receivedBytes = 0;
    int sentBytes = 0;
    if (requestContext.hasEntity()) {
      try {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        CountingOutputStream cus = new CountingOutputStream(byteArrayOutputStream);
        OBJECT_MAPPER.writeValue(cus, requestContext.getEntity());
        cus.close();
        sentBytes = (int) cus.getCount();
      } catch (Exception e) {
        LOGGER.warning("Failed to measure request body size: " + e.getMessage());
      }
    }
    if (responseContext.hasEntity()) {
      receivedBytes = responseContext.getLength();
      if (receivedBytes <= 0) {
        // Read the response entity stream to measure its size
        InputStream inputStream = responseContext.getEntityStream();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        receivedBytes = (int) ByteStreams.copy(inputStream, byteArrayOutputStream);
      }
    }

    responseContext.getHeaders().add("sent-bytes", String.valueOf(sentBytes));
    responseContext.getHeaders().add("received-bytes", String.valueOf(receivedBytes));
  }
}
