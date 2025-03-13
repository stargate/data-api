package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.util.recordable.Jsonable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Describes how {@link TraceMessage}'s can be tracked to diagnose how a request is processed.
 *
 * <p>The {@link #NO_TRACING} default drops the messages and does not return a trace, use an
 * implementation like {@link DefaultRequestTracing} when tracing is enabled.
 *
 * <p>Code should get the current implementation from {@link CommandContext#requestTracing()} and
 * avoid doing work until the message supplied is called.
 */
public interface RequestTracing {

  /**
   * Default implementation that will not generate trace messages and returns an empty value from
   * {@link #getTrace()}
   */
  RequestTracing NO_TRACING =
      new RequestTracing() {

        @Override
        public boolean enabled() {
          return false;
        }

        @Override
        public void maybeTrace(Supplier<TraceMessage> messageSupplier) {}

        @Override
        public void maybeTrace(Function<ObjectMapper, TraceMessage> messageSupplier) {}

        @Override
        public Optional<JsonNode> getTrace() {
          return Optional.empty();
        }
      };

  boolean enabled();

  /**
   * Call to add a {@link TraceMessage} to the trace, the implementation will only call the
   * messageSupplier if tracing is enabled.
   */
  void maybeTrace(Supplier<TraceMessage> messageSupplier);

  /**
   * Call to add a {@link TraceMessage} to the trace, the implementation will only call the
   * messageSupplier if tracing is enabled.
   *
   * <p>This overload passed in a {@link ObjectMapper} for the supplier to use to make a JsonNode
   * for the message.
   */
  void maybeTrace(Function<ObjectMapper, TraceMessage> messageSupplier);

  /** Convenience method for {@link #maybeTrace(Supplier)} */
  default void maybeTrace(String message) {
    maybeTrace(() -> new TraceMessage(message));
  }

  /** Convenience method for {@link #maybeTrace(Supplier)} */
  default void maybeTrace(String message, Recordable recordable) {
    maybeTrace(() -> new TraceMessage(message, recordable));
  }

  default void maybeTrace(String message, Supplier<Recordable> recordable) {
    maybeTrace(() -> new TraceMessage(message, recordable.get()));
  }

  /**
   * Called to get the complete request trace to be included in the response.
   *
   * @return A non-empty value if tracing was enabled, return an empty value if there is no trace.
   */
  Optional<JsonNode> getTrace();

  /**
   * A message added to be addd to the request tracing.
   *
   * @param message Text description of the event, useful to have a few data items like the number
   *     of tasks. More complex data should be included in the data
   * @param data Json to include as the "data" part of the trace, this can be non-trivial like the
   *     total description of the task group. Implementations may at time ignore the data if they
   *     want to limit bandwidth, so include some details in the message as well.
   */
  record TraceMessage(String message, JsonNode data) {

    public TraceMessage(String message) {
      this(message, (JsonNode) null);
    }

    /**
     * Creates a new message from a {@link Recordable} object, useful when the message includes the
     * state of internal objects.
     *
     * @param message Text message
     * @param recordable Required, {@link Recordable} object that will be recorded to Json using
     *     {@link Jsonable}
     */
    public TraceMessage(String message, Recordable recordable) {
      this(
          message,
          Jsonable.toJson(Objects.requireNonNull(recordable, "recordable must not be null")));
    }
  }
}
