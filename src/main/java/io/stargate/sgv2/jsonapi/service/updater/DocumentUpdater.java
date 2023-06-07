package io.stargate.sgv2.jsonapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.List;

/** Updates the document read from the database with the updates came as part of the request. */
public record DocumentUpdater(
    List<UpdateOperation> updateOperations,
    ObjectNode replaceDocument,
    JsonNode replaceDocumentId,
    UpdateType updateType) {
  /**
   * Construct to create updater using update clause
   *
   * @param updateDef
   * @return
   */
  public static DocumentUpdater construct(UpdateClause updateDef) {
    return new DocumentUpdater(updateDef.buildOperations(), null, null, UpdateType.UPDATE);
  }

  /**
   * Construct to create updater using replace document
   *
   * @param replaceDocument
   * @return
   */
  public static DocumentUpdater construct(ObjectNode replaceDocument) {
    JsonNode replaceDocumentId = replaceDocument.remove(DocumentConstants.Fields.DOC_ID);
    return new DocumentUpdater(null, replaceDocument, replaceDocumentId, UpdateType.REPLACE);
  }

  /**
   * @param readDocument Document to update
   * @param docInserted True if document was just created (inserted); false if updating existing
   *     document
   */
  public DocumentUpdaterResponse apply(JsonNode readDocument, boolean docInserted) {
    ObjectNode docToUpdate = (ObjectNode) readDocument;
    if (UpdateType.UPDATE == updateType) {
      boolean modified = update(docToUpdate, docInserted);
      return new DocumentUpdaterResponse(readDocument, modified);
    } else {
      boolean modified = replace(docToUpdate, docInserted);
      return new DocumentUpdaterResponse(readDocument, modified);
    }
  }

  /**
   * Will be used for update commands
   *
   * @param docToUpdate
   * @param docInserted
   * @return
   */
  private boolean update(ObjectNode docToUpdate, boolean docInserted) {
    boolean modified = false;
    for (UpdateOperation updateOperation : updateOperations) {
      if (updateOperation.shouldApplyIf(docInserted)) {
        modified |= updateOperation.updateDocument(docToUpdate);
      }
    }
    return modified;
  }

  /**
   * Will be used for findOneAndReplace
   *
   * @param docToUpdate
   * @param docInserted
   * @return
   */
  private boolean replace(ObjectNode docToUpdate, boolean docInserted) {
    // Do deep clone so we can remove _id field and check
    ObjectNode compareDoc = docToUpdate.deepCopy();
    JsonNode idNode = compareDoc.remove(DocumentConstants.Fields.DOC_ID);
    // The replace document cannot specify an _id value that differs from the replaced document.
    if (replaceDocumentId != null && idNode != null) {
      if (!JsonUtil.equalsOrdered(replaceDocumentId, idNode)) {
        // throw error id cannot be different
        throw new JsonApiException(ErrorCode.DOCUMENT_REPLACE_DIFFERENT_DOCID);
      }
    }
    // In case there is no difference between document return modified as false, so db update
    // doesn't happen
    if (JsonUtil.equalsOrdered(compareDoc, replaceDocument())) {
      return false;
    }
    // remove all data and add _id as first field; either from original document or from replacement
    docToUpdate.removeAll();
    if (idNode != null) {
      docToUpdate.set(DocumentConstants.Fields.DOC_ID, idNode);
    } else if (replaceDocumentId != null) {
      docToUpdate.set(DocumentConstants.Fields.DOC_ID, replaceDocumentId);
    }
    docToUpdate.setAll(replaceDocument());
    // return modified flag as true
    return true;
  }

  public record DocumentUpdaterResponse(JsonNode document, boolean modified) {}

  private enum UpdateType {
    UPDATE,
    REPLACE
  }
}
