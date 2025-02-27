package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.common.base.Stopwatch;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    session.addMessage(messageSupplier.get());
  }

  @Override
  public Optional<ObjectNode> getTrace() {
    return Optional.of(OBJECT_MAPPER.convertValue(session, ObjectNode.class));
  }

  private static class TraceSession {

    private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

    private final String requestId;
    private final String tenantId;

    private final Stopwatch watch;
    private final Date startedAt;
    private final List<TraceEvent> events = new ArrayList<>();

    TraceSession(String requestId, String tenantId) {
      this.requestId = requestId;
      this.tenantId = tenantId;

      watch = Stopwatch.createStarted();
      startedAt = new Date();
    }

    /**
     * Get the number of eleapsed microseconds since the start of the session, clamped to {@link
     * Integer#MAX_VALUE}
     *
     * <p>We dont need to do this for JSON representation, we could use a long for that. But clamped
     * to int for compatibility with the CQL tracing and {@link Integer#MAX_VALUE} number of
     * microseconds is:
     *
     * <pre>
     *   2,147,483,647 microseconds is approximately 35 minutes and 47.48 seconds.
     * </pre>
     *
     * @return Number of microseconds since the start of the session clamped to {@link
     *     Integer#MAX_VALUE}
     */
    private int elapsedMicroseconds() {
      long elapsed = watch.elapsed(TimeUnit.MICROSECONDS);
      return elapsed < Integer.MAX_VALUE ? (int) elapsed : Integer.MAX_VALUE;
    }

    private void addMessage(RequestTracing.TraceMessage traceMessage) {
      Objects.requireNonNull(traceMessage, "messageSupplier must not be null");

      events.add(
          new TraceEvent(
              UUID_V7_GENERATOR.generate(),
              new Date(),
              elapsedMicroseconds(),
              traceMessage.message(),
              null));
    }

    public String getRequestId() {
      return requestId;
    }

    public String getTenantId() {
      return tenantId;
    }

    public Date getStartedAt() {
      return startedAt;
    }

    public int getDurationUs() {
      return elapsedMicroseconds();
    }

    public List<TraceEvent> getEvents() {
      return events;
    }
  }

  private record TraceEvent(
      UUID eventId, Date timestamp, int elapsedUs, String message, String data) {}
  ;
}
