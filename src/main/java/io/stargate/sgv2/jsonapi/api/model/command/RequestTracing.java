package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.Jsonable;
import io.stargate.sgv2.jsonapi.util.Recordable;
import java.util.Optional;
import java.util.function.Supplier;

public interface RequestTracing {

  RequestTracing NO_TRACING =
      new RequestTracing() {
        @Override
        public void maybeTrace(Supplier<TraceMessage> messageSupplier) {}

        @Override
        public Optional<ObjectNode> getTrace() {
          return Optional.empty();
        }
      };

  default void maybeTrace(String message) {
    maybeTrace(() -> new TraceMessage(message, null));
  }

  default void maybeTrace(String message, Recordable recordable) {
    maybeTrace(() -> new TraceMessage(message, recordable));
  }

  void maybeTrace(Supplier<TraceMessage> messageSupplier);

  Optional<ObjectNode> getTrace();

  record TraceMessage(String message, Recordable recordable, JsonNode data) {

    public TraceMessage {
      if (recordable != null && data == null) {
        data = Jsonable.toJson(recordable);
      }
    }

    public TraceMessage(String message, Recordable recordable) {
      this(message, recordable, null);
    }
  }
}
