package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.Jsonable;
import io.stargate.sgv2.jsonapi.util.Recordable;
import java.util.Optional;
import java.util.function.Supplier;

public interface RequestTracing {

  RequestTracing NO_TRACING = new RequestTracing() {};

  default void maybeTrace(Supplier<TraceMessage> messageSupplier) {}

  default Optional<ObjectNode> getTrace() {
    return Optional.empty();
  }

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
