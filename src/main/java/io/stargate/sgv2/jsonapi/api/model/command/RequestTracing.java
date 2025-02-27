package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.function.Supplier;

public interface RequestTracing {

  RequestTracing NO_TRACING = new RequestTracing() {};

  default void maybeTrace(Supplier<TraceMessage> messageSupplier) {}

  default Optional<ObjectNode> getTrace() {
    return Optional.empty();
  }

  record TraceMessage(String message, String data) {}
}
