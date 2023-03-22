package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;

/*
 * All the commands that need Projection definitions will have to implement this.
 */
public interface Projectable {
  JsonNode projectionDefinition();
}
