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
   * This method is the entrance for first level update or replace. First level means it won't
   * vectorize if needed, but will warp an EmbeddingUpdateOperation in the DocumentUpdaterResponse
   * to do the following embedding update.
   *
   * @param readDocument Document to update
   * @param docInserted True if document was just created (inserted); false if updating existing
   *     document
   */
  public DocumentUpdaterResponse apply(JsonNode readDocument, boolean docInserted) {
    ObjectNode docToUpdate = (ObjectNode) readDocument;
    if (UpdateType.UPDATE == updateType) {
      return update(docToUpdate, docInserted);
    } else {
      return replace(docToUpdate, docInserted);
    }
  }

  /**
   * Will be used for update commands. This is first level replace. This method will replace the
   * document, but won't re-vectorize yet(detail in updateEmbeddingVector method)
   *
   * @param docToUpdate
   * @param docInserted
   * @return
   */
  private DocumentUpdaterResponse update(ObjectNode docToUpdate, boolean docInserted) {
    boolean modified = false;
    EmbeddingUpdateOperation embeddingUpdateOperation = null;
    for (UpdateOperation updateOperation : updateOperations) {
      if (updateOperation.shouldApplyIf(docInserted)) {
        final UpdateOperation.UpdateOperationResult updateOperationResult =
            updateOperation.updateDocument(docToUpdate);
        modified |= updateOperationResult.modified();
        embeddingUpdateOperation = updateOperationResult.embeddingUpdateOperation();
      }
    }
    return new DocumentUpdaterResponse(docToUpdate, modified, embeddingUpdateOperation);
  }

  /**
   * Will be used for findOneAndReplace. This is first level replace. This method will replace the
   * document, but won't re-vectorize yet(detail in updateEmbeddingVector method)
   *
   * @param docToUpdate
   * @param docInserted
   * @return
   */
  private DocumentUpdaterResponse replace(ObjectNode docToUpdate, boolean docInserted) {
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

    EmbeddingUpdateOperation embeddingUpdateOperation = null;
    JsonNode vectorizeNode =
        replaceDocument.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
    if (vectorizeNode != null) {
      // If replaceDocument has $vectorize as null value or blank text value, also set $vector as
      // null value here.
      if (vectorizeNode.isNull()) {
        // if $vectorize is null value, update $vector as null
        replaceDocument.put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
      } else if (!vectorizeNode.isTextual()) {
        // if $vectorize is not textual value
        throw ErrorCode.INVALID_VECTORIZE_VALUE_TYPE.toApiException();
      } else if (vectorizeNode.asText().isBlank()) {
        // $vectorize is blank text value, set $vector as null value, no need to vectorize
        replaceDocument.put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
      } else {
        // if $vectorize is textual and not blank, create embeddingUpdateOperation
        embeddingUpdateOperation = new EmbeddingUpdateOperation(vectorizeNode.asText());
      }
    }

    // In case there is no difference between document return modified as false, so db update
    // doesn't happen
    if (JsonUtil.equalsOrdered(compareDoc, replaceDocument())) {
      return new DocumentUpdaterResponse(docToUpdate, false, null);
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
    return new DocumentUpdaterResponse(docToUpdate, true, embeddingUpdateOperation);
  }

  /**
   * This method is used for potential vectorize There may exist a not-null embeddingUpdateOperation
   * in responseBeforeVectorize param, then use dataVectorizer to vectorize the content and then use
   * embeddingUpdateOperation to update the document's $vector field
   *
   * @param responseBeforeVectorize response before vectorize
   * @param dataVectorizer dataVectorizer
   * @return Uni<DocumentUpdaterResponse>
   */
  public Uni<DocumentUpdaterResponse> updateEmbeddingVector(
      DocumentUpdaterResponse responseBeforeVectorize, DataVectorizer dataVectorizer) {
    final EmbeddingUpdateOperation embeddingUpdateOperation =
        responseBeforeVectorize.embeddingUpdateOperation();
    if (embeddingUpdateOperation == null) {
      return Uni.createFrom().item(responseBeforeVectorize);
    }
    return dataVectorizer
        .vectorize(embeddingUpdateOperation.vectorizeContent())
        .onItem()
        .transformToUni(
            vector -> {
              embeddingUpdateOperation.updateDocument(responseBeforeVectorize.document, vector);
              return Uni.createFrom().item(responseBeforeVectorize);
            });
  }

  /**
   * The documentUpdaterResponse has the updated document, boolean flag to indicate the document is
   * modified or not, an embeddingUpdateOperation to update the embedding
   */
  public record DocumentUpdaterResponse(
      JsonNode document, boolean modified, EmbeddingUpdateOperation embeddingUpdateOperation) {}

  private enum UpdateType {
    UPDATE,
    REPLACE
  }
}
