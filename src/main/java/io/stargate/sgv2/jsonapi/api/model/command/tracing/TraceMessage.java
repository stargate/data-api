package io.stargate.sgv2.jsonapi.api.model.command.tracing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.stargate.sgv2.jsonapi.util.recordable.Jsonable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;

/**
 * A message added to be addd to the request tracing.
 *
 * @param message Text description of the event, useful to have a few data items like the number of
 *     tasks. More complex data should be included in the data
 * @param jsonData Json to include as the "data" part of the trace, this can be non-trivial like the
 *     total description of the task group. Implementations may at time ignore the data if they want
 *     to limit bandwidth, so include some details in the message as well.
 * @param recordableData A {@link Recordable} object, if the data is needed for the trace message it
 *     will be dumped to JSON using {@link Jsonable#toJson(Recordable)}. The advantage of this is
 *     that the data will not be built until we know we need it to build the trace for the response.
 */
public record TraceMessage(String message, JsonNode jsonData, Recordable recordableData) {

  public TraceMessage(String message) {
    this(message, (JsonNode) null, (Recordable) null);
  }

  public TraceMessage(String message, JsonNode jsonData) {
    this(message, jsonData, null);
  }

  public TraceMessage(String message, Recordable recordable) {
    this(message, null, recordable);
  }

  JsonNode dataOrRecordable() {
    if (jsonData != null) {
      return jsonData;
    }
    if (recordableData != null) {
      return Jsonable.toJson(recordableData);
    }
    return NullNode.getInstance();
  }
}
