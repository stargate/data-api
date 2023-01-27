package io.stargate.sgv3.docsapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation;
import java.util.List;

/** Updates the document read from the database with the updates came as part of the request. */
public record DocumentUpdater(List<UpdateOperation> updateOperations) {
  public static DocumentUpdater construct(UpdateClause updateDef) {
    return new DocumentUpdater(updateDef.buildOperations());
  }

  public JsonNode applyUpdates(JsonNode readDocument) {
    ObjectNode docToUpdate = (ObjectNode) readDocument;
    updateOperations.forEach(u -> u.updateDocument(docToUpdate));
    return readDocument;
  }
}
