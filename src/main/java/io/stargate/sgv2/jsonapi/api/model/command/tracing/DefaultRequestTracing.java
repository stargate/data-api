package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Request Tracing that tracks the messages in a session. */
public class DefaultRequestTracing implements RequestTracing {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRequestTracing.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TraceSession session;

  public DefaultRequestTracing(String requestId, String tenantId) {
    Objects.requireNonNull(requestId, "requestId must not be null");
    Objects.requireNonNull(tenantId, "tenantId must not be null");

    // not checking if they are empty strings, that is the responsibility of the caller
    this.session = new TraceSession(requestId, tenantId);
  }

  @Override
  public void maybeTrace(Supplier<RequestTracing.TraceMessage> messageSupplier) {
    Objects.requireNonNull(messageSupplier, "messageSupplier must not be null");

    var message = messageSupplier.get();
    if (message == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "TraceMessage supplier returned a null message, ignoring: messageSupplier.class:{}",
            messageSupplier.getClass());
      }
      return;
    }
    session.addMessage(message);
  }

  @Override
  public Optional<JsonNode> getTrace() {
    return Optional.of(OBJECT_MAPPER.convertValue(session, JsonNode.class));
  }
}
