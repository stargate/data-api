package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.common.base.Stopwatch;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Tracing Session contains the trace information for a single request.
 *
 * <p>The {@link RequestTracing} interface does not require the use of {@link TraceSession} or
 * {@link TraceEvent} implementations can whatever they want. This is used by {@link
 * DefaultRequestTracing}.
 *
 * <p>This and the {@link TraceEvent} are designed to be serialised by Jackson.
 */
class TraceSession {

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
   * <p>We don't need to do this for JSON representation, we could use a long for that. But clamped
   * to int for compatibility with the CQL tracing and the {@link Integer#MAX_VALUE} number of
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

  /**
   * Adds a message to the session.
   *
   * @param traceMessage Required, the message to add.
   * @return The {@link TraceEvent} that was created to hold the message.
   */
  TraceEvent addMessage(RequestTracing.TraceMessage traceMessage) {
    Objects.requireNonNull(traceMessage, "messageSupplier must not be null");

    var event =
        new TraceEvent(
            UUID_V7_GENERATOR.generate(),
            new Date(),
            elapsedMicroseconds(),
            traceMessage.message(),
            traceMessage.data());

    events.add(event);
    return event;
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

  public int getDurationMicroseconds() {
    return elapsedMicroseconds();
  }

  public List<TraceEvent> getEvents() {
    return events;
  }

  /**
   * An even in the request trace, based on the {@link
   * io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing.TraceMessage} with context
   * for when it happened.
   */
  record TraceEvent(
      UUID eventId, Date timestamp, int elapsedMicroseconds, String message, JsonNode data) {}
}
