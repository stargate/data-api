package io.stargate.sgv3.docsapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation;

/**
 * Updates the document read from the database with the updates came as part of the request.
 *
 * @param updateClause
 */
public record DocumentUpdater(UpdateClause updateClause) {
  public JsonNode applyUpdates(JsonNode readDocument) {
    ObjectNode updatableObjectNode = ((ObjectNode) readDocument);
    updateClause
        .updateOperations()
        .forEach(updateOperation -> updateDocument(updateOperation, updatableObjectNode));
    return readDocument;
  }

  /**
   * Based on the value type update the value to the read document
   *
   * @param updateOperation
   * @param updatableObjectNode
   */
  private void updateDocument(UpdateOperation updateOperation, ObjectNode updatableObjectNode) {

    switch (updateOperation.operator()) {
      case SET -> {
        updatableObjectNode.put(updateOperation.path(), updateOperation.value());
        return;
      }
      case UNSET -> {
        updatableObjectNode.remove(updateOperation.path());
        return;
      }
    }
  }
}
