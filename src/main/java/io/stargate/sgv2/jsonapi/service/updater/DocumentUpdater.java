package io.stargate.sgv2.jsonapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.*;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Updates the document read from the database with the updates came as part of the request.
 * DocumentUpdater construct postpone from commandResolver level to operation level, since we want
 * the vectorize as needed, only vectorize when there are documents found or upsert
 *
 * @param updateClause - vectorize it as needed before building update operations
 * @param replaceDocument - replaceDocument to replace the one read from DB
 * @param replaceDocumentId - documentId from replaceDocument
 * @param updateType - UPDATE/REPLACE
 */
public record DocumentUpdater(
    // buildOperations will be executed when apply to update
    UpdateClause updateClause,
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
    return new DocumentUpdater(updateDef, null, null, UpdateType.UPDATE);
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
    List<UpdateOperation> updateOperations = updateClause.buildOperations();
    for (UpdateOperation updateOperation : updateOperations) {
      if (updateOperation.shouldApplyIf(docInserted)) {
        modified |= updateOperation.updateDocument(docToUpdate);
      }
    }
    return modified;
  }

  /**
   * vectorize UpdateClause as needed, only when documents are found or upsert will be used by
   * updateOne, findOneAndUpdate, updateMany
   *
   * @param dataVectorizer
   */
  public void vectorizeUpdateClause(DataVectorizer dataVectorizer) {
    try {
      dataVectorizer
          .vectorizeUpdateClause(updateClause)
          .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
          .subscribeAsCompletionStage()
          .get();
    } catch (Exception e) {
      if (e instanceof ExecutionException exception) {
        if (exception.getCause() instanceof JsonApiException jsonApiException) {
          throw jsonApiException;
        }
      }
      throw new RuntimeException(e);
    }
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

  /**
   * vectorize replacementDocument as needed, only when document is found will be used by
   * findOneAndReplace
   *
   * @param dataVectorizer
   */
  public void vectorizeTheReplacementDocument(DataVectorizer dataVectorizer) {
    // TODO: check if $vectorize must be at first level
    try {
      dataVectorizer
          .vectorize(List.of(replaceDocument))
          .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
          .subscribeAsCompletionStage()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public record DocumentUpdaterResponse(JsonNode document, boolean modified) {}

  public enum UpdateType {
    UPDATE,
    REPLACE
  }
}
