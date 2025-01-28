package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Value type enclosing JSON-decoded representation of something to resolve into fully definition,
 * such as filter or sort clause. Needed to let Quarkus/Jackson decode textual JSON into
 * intermediate form ({@link JsonNode}) from which actual deserialization and validation can be
 * deferred until we have context we need
 */
public abstract class JsonDefinition {
  /** The wrapped JSON value */
  private final JsonNode json;

  protected JsonDefinition(JsonNode json) {
    this.json = json;
  }

  /**
   * @return JSON value that specifies the object
   */
  public JsonNode json() {
    return json;
  }
}
