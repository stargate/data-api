package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;

/**
 * Value type enclosing JSON-encoded representation of something to build into full model object,
 * such as filter or sort clause. Needed to let Quarkus/Jackson decode textual JSON into
 * intermediate form ({@link JsonNode}) from which actual deserialization and validation can be
 * deferred until we have context we need
 *
 * @see io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition
 * @see io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition
 * @param <T> Type of the value object to be deserialized from the JSON value
 */
public abstract class JsonDefinition<T> {
  /** The wrapped JSON value */
  private final JsonNode json;

  protected JsonDefinition(JsonNode json) {
    this.json = Objects.requireNonNull(json);
  }

  /**
   * Method for lazy deserialization of the JSON value into a {@link T} instance, usually a {@code
   * Clause} of some type.
   *
   * @param ctx Context passed to the builder
   * @return Fully processed {@link T} instance based on this definition.
   */
  public abstract T build(CommandContext<?> ctx);

  /**
   * @return JSON value that contains the definition of value object to build
   */
  @VisibleForTesting
  public JsonNode json() {
    return json;
  }
}
