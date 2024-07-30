package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.projection.TableProjectionDefinition;

/*
 * All the commands that need Projection definitions will have to implement this.
 */
public interface Projectable {
  JsonNode projectionDefinition();

  default DocumentProjector buildProjector() {
    return buildProjector(false);
  }

  default TableProjectionDefinition tableProjectionDefinition() {
    return TableProjectionDefinition.createFromDefinition(projectionDefinition());
  }

  default DocumentProjector buildProjector(boolean includeSimilarity) {
    return DocumentProjector.createFromDefinition(projectionDefinition(), includeSimilarity);
  }
}
