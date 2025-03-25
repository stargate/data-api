package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes how {@link TraceMessage}'s can be recorded to trace how a request is processed.
 *
 * <p>The {@link #NO_OP} default drops the messages and does not return a trace, use an
 * implementation like {@link DefaultRequestTracing} when tracing is enabled.
 *
 * <p>Code should get the current implementation from {@link CommandContext#requestTracing()} and
 * avoid doing work until the message supplied is called.
 */
public abstract class RequestTracing {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestTracing.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Default implementation that is not enabled, will ignore calls to maybeTrace and not return a
   * session
   */
  public static final RequestTracing NO_OP =
      new RequestTracing(false) {
        @Override
        protected void traceMessage(TraceMessage message) {}

        @Override
        public Optional<TraceSession> getSession() {
          return Optional.empty();
        }
      };

  private final boolean enabled;

  protected RequestTracing(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Returns true if tracing is enabled, false otherwise.
   *
   * <p>Code can check this first is there is work to do, or use one of the {@link #maybeTrace}
   * methods that takes a supplier that will only be called if tracing is enabled.
   *
   * @return true if tracing is enabled, false otherwise.
   */
  public boolean enabled() {
    return enabled;
  }

  /**
   * Call to add a {@link TraceMessage} to the trace, the implementation will only call the
   * messageSupplier if tracing is enabled.
   */
  public void maybeTrace(Supplier<TraceMessage> messageSupplier) {
    Objects.requireNonNull(messageSupplier, "messageSupplier must not be null");

    if (!enabled) {
      return;
    }

    var message = messageSupplier.get();
    if (message == null) {
      logNullMessage(messageSupplier.getClass());
      return;
    }
    traceMessage(message);
  }

  /**
   * Call to add a {@link TraceMessage} to the trace, the implementation will only call the
   * messageSupplier if tracing is enabled.
   *
   * <p>This overload passed in a {@link ObjectMapper} for the supplier to use to make a JsonNode
   * for the message.
   */
  public void maybeTrace(Function<ObjectMapper, TraceMessage> messageSupplier) {
    Objects.requireNonNull(messageSupplier, "messageSupplier must not be null");

    if (!enabled) {
      return;
    }

    var message = messageSupplier.apply(OBJECT_MAPPER);
    if (message == null) {
      logNullMessage(messageSupplier.getClass());
      return;
    }
    traceMessage(message);
  }

  /** Convenience method for {@link #maybeTrace(Supplier)} with just a message */
  public void maybeTrace(String message) {
    if (!enabled) {
      return;
    }
    maybeTrace(() -> new TraceMessage(message));
  }

  /**
   * Convenience method for {@link #maybeTrace(Supplier)} with a message and a {@link Recordable}
   */
  public void maybeTrace(String message, Recordable recordable) {
    if (!enabled) {
      return;
    }
    maybeTrace(() -> new TraceMessage(message, recordable));
  }

  public void maybeTrace(String message, Supplier<Recordable> recordable) {
    if (!enabled) {
      return;
    }
    maybeTrace(() -> new TraceMessage(message, recordable.get()));
  }

  // ==================================================================================================================
  // Subclass API and implementation
  // ==================================================================================================================

  /**
   * Called when there is a trace message to be recorded.
   *
   * @param message The message to record
   */
  protected abstract void traceMessage(TraceMessage message);

  /**
   * Called to get the {@link TraceSession} that messages have been recorded to.
   *
   * @return A non-empty value if tracing was enabled, return an empty value if there is no trace.
   */
  public abstract Optional<TraceSession> getSession();

  protected static void logNullMessage(Class<?> supplierClass) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "TraceMessage supplier returned a null message, ignoring: messageSupplier.getClass()={}",
          supplierClass);
    }
  }
}
