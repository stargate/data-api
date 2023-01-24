package io.stargate.sgv3.docsapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.math.BigDecimal;

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

  private void updateDocument(UpdateOperation<?> updateOperation, ObjectNode updatableObjectNode) {

    switch (updateOperation.operator()) {
      case SET -> {
        switch (updateOperation.value().valueType()) {
          case BOOLEAN -> {
            updatableObjectNode.put(
                updateOperation.path(),
                ((UpdateOperation<Boolean>) updateOperation).value().value());
            return;
          }
          case NUMBER -> {
            updatableObjectNode.put(
                updateOperation.path(),
                ((UpdateOperation<BigDecimal>) updateOperation).value().value());
            return;
          }
          case STRING -> {
            updatableObjectNode.put(
                updateOperation.path(),
                ((UpdateOperation<String>) updateOperation).value().value());
            return;
          }
          case NULL -> {
            updatableObjectNode.put(updateOperation.path(), (String) null);
            return;
          }
          default -> {
            throw new DocsException(ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE);
          }
        }
      }
      case UNSET -> {
        updatableObjectNode.remove(updateOperation.path());
        return;
      }
    }
  }
}
