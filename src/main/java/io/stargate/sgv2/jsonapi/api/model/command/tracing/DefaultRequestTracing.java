package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Request Tracing that tracks the messages in a session. */
public class DefaultRequestTracing extends RequestTracing {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRequestTracing.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TraceSession session;

  public DefaultRequestTracing(String requestId, Tenant tenantId, boolean includeData) {
    super(true);

    Objects.requireNonNull(requestId, "requestId must not be null");
    Objects.requireNonNull(tenantId, "tenantId must not be null");

    // not checking if they are empty strings, that is the responsibility of the caller
    session = new TraceSession(requestId, tenantId, includeData);
  }

  @Override
  protected void traceMessage(TraceMessage message) {
    session.addMessage(message);
  }

  @Override
  public Optional<TraceSession> getSession() {
    return Optional.of(session);
  }
}
