package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.client.ClientResponseFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

public class NetworkUsageInterceptor implements ClientRequestFilter, ClientResponseFilter {

  private static final Logger LOGGER = Logger.getLogger(NetworkUsageInterceptor.class.getName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // Jackson object mapper

  @Override
  public void filter(ClientRequestContext requestContext) throws IOException {

    // **3. Calculate Body size (if present)**
    if (requestContext.hasEntity()) {
      try {
        byte[] requestBody = OBJECT_MAPPER.writeValueAsBytes(requestContext.getEntity());
        requestContext.setProperty("sentBytes", requestBody.length);
      } catch (Exception e) {
        LOGGER.warning("Failed to measure request body size: " + e.getMessage());
      }
    }
  }

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext)
      throws IOException {
    int receivedBytes = 0;
    int sentBytes = (int) requestContext.getProperty("sentBytes");
    if (responseContext.hasEntity()) {
      // Read the response entity stream to measure its size
      InputStream inputStream = responseContext.getEntityStream();
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        byteArrayOutputStream.write(buffer, 0, bytesRead);
        receivedBytes += bytesRead;
      }
      responseContext.setEntityStream(
          new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    }
    LOGGER.info("Received Bytes: " + receivedBytes);
    responseContext.getHeaders().add("sent-bytes", String.valueOf(sentBytes));
    responseContext.getHeaders().add("received-bytes", String.valueOf(receivedBytes));
  }
}
