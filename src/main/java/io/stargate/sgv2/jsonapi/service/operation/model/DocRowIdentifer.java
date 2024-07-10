package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.annotation.JsonValue;

// TODO AARON base for antying to identify a doc or a row
public interface DocRowIdentifer {

  /** Method called by JSON serializer to get value to include in JSON output. */
  @JsonValue
  Object value();
}
