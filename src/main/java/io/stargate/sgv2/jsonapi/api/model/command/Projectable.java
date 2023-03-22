package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;

/*
 * All the commands that need Projection definitions will have to implement this.
 */
public interface Projectable {
  JsonNode projectionDefinition();

  default DocumentProjector buildProjector() {
    return DocumentProjector.createFromDefinition(projectionDefinition());
  }
}
