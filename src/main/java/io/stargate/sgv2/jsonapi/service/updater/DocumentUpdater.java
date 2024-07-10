package io.stargate.sgv2.jsonapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.*;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
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
   * This method is the entrance for first level update or replace. first level means it won't
   * vectorize and update $vectorize so the updatedDocument returned in DocumentUpdaterResponse will
   * leave $vectorize unchanged
   *
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
   * Will be used for update commands. This method won't update $vectorize (detail in
   * applyUpdateVectorize method)
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
   * Will be used for findOneAndReplace. This method will replace $vectorize, but won't re-vectorize
   * and replace $vector(detail in applyUpdateVectorize method)
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

    // If replaceDocument has $vectorize as null value, also set $vector as null here.
    // This is because we need to do a comparison for compareDoc and replaceDocument later
    JsonNode vectorizeNode =
        replaceDocument.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
    if (vectorizeNode != null && vectorizeNode.isNull()) {
      ((ObjectNode) replaceDocument)
          .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
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
    //    // restore the original $vectorize
    //    docToUpdate.put(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
    // vectorizeNode.asText());
    // return modified flag as true
    return true;
  }

  /**
   * This method is the entrance for second level update or replace. This level will vectorize on
   * demand and change $vectorize and $vector accordingly.
   *
   * @param readDocument Document to update(This document may has been updated once, detail in first
   *     level update)
   * @param docInserted True if document was just created (inserted); false if updating existing
   *     document
   */
  public Uni<DocumentUpdaterResponse> applyUpdateVectorize(
      JsonNode readDocument, boolean docInserted, DataVectorizer dataVectorizer) {
    if (UpdateType.UPDATE == updateType) {
      for (UpdateOperation updateOperation : updateOperations) {
        if (updateOperation.shouldApplyIf(docInserted)
            && updateOperation instanceof SetOperation setOperation) {
          // filtering out the setOperation
          // try to vectorize on demand and change $vectorize and $vector accordingly.
          return setOperation
              .updateVectorize(readDocument, dataVectorizer)
              .onItem()
              .transformToUni(
                  modified -> {
                    return Uni.createFrom()
                        .item(new DocumentUpdaterResponse(readDocument, modified));
                  });
        }
      }
    }
    if (UpdateType.REPLACE == updateType) {
      // Only need to vectorize when:
      // replaceDocument has $vectorize(not null), this is consistent with previous behaviour
      // This means even if $vectorize has no diff between readDoc and replacementDoc, we still
      // re-vectorize
      if (!replaceDocument.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD).isNull()) {
        return dataVectorizer
            // replacement also considered as update, set isUpdateCommand flag as true
            .vectorize(List.of(readDocument), true)
            .onItem()
            .transformToUni(
                modified -> {
                  return Uni.createFrom().item(new DocumentUpdaterResponse(readDocument, modified));
                });
      }
    }
    // there is no setOperation, so won't modify anything
    return Uni.createFrom().item(new DocumentUpdaterResponse(readDocument, false));
  }

  public record DocumentUpdaterResponse(JsonNode document, boolean modified) {}

  public enum UpdateType {
    UPDATE,
    REPLACE
  }
}
