package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.common.base.Stopwatch;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Tracing Session contains the trace information for a single request.
 *
 * <p>
 *
 * <p>NOTE: Design to be thread safe so that multiple Uni stages can add messages to the trace at
 * the same time. The use the {@link io.stargate.sgv2.jsonapi.util.recordable.Recordable} interface
 * to record the state of objects, because this also Synchronised.
 */
public class TraceSession implements Recordable {

  private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();
  private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

  private static final TextNode DATA_OMITTED =
      new TextNode(
          "Data Omitted - for full data use header: %s=true"
              .formatted(ApiFeature.REQUEST_TRACING_FULL.httpHeaderName()));

  private final String requestId;
  private final Tenant tenant;

  private final Stopwatch watch;
  private final Instant startedAt;
  private final List<TraceEvent> events = new ArrayList<>();

  private final boolean includeData;

  TraceSession(String requestId, Tenant tenant, boolean includeData) {
    this.requestId = requestId;
    this.tenant = tenant;
    this.includeData = includeData;

    watch = Stopwatch.createStarted();
    startedAt = Instant.now();
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
  TraceEvent addMessage(TraceMessage traceMessage) {
    Objects.requireNonNull(traceMessage, "traceMessage must not be null");

    var event =
        new TraceEvent(
            UUID_V7_GENERATOR.generate(),
            Instant.now(),
            elapsedMicroseconds(),
            traceMessage.message(),
            includeData ? traceMessage.dataOrRecordable() : DATA_OMITTED);

    synchronized (events) {
      events.add(event);
    }
    return event;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    synchronized (events) {
      return dataRecorder
          .append("requestId", requestId)
          .append("tenant", tenant.toString())
          .append("startedAt", ISO_FORMATTER.format(startedAt))
          .append("durationMicroseconds", elapsedMicroseconds())
          .append("events", events);
    }
  }

  /**
   * An even in the request trace, based on the {@link TraceMessage} with context for when it
   * happened.
   */
  record TraceEvent(
      UUID eventId, Instant timestamp, int elapsedMicroseconds, String message, JsonNode data)
      implements Recordable {
    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("eventId", eventId)
          .append("timestamp", ISO_FORMATTER.format(timestamp))
          .append("elapsedMicroseconds", elapsedMicroseconds)
          .append("message", message)
          .append("data", data);
    }
  }
}
