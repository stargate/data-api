package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScoredDocumentMerger {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScoredDocumentMerger.class);

  private final Map<JsonNode, ScoredDocument> mergedDocuments;
  private int seenDocumentsCount = 0;
  private int droppedDocumentsCount = 0;
  private final String passageField;
  private final DocumentProjector userProjection;

  ScoredDocumentMerger(int initialCapacity, String passageField, DocumentProjector userProjection) {
    mergedDocuments = new HashMap<>(initialCapacity);

    this.userProjection = userProjection;
    this.passageField = passageField;
  }

  int seenDocuments() {
    return seenDocumentsCount;
  }

  int droppedDocuments() {
    return droppedDocumentsCount;
  }

  List<ScoredDocument> mergedDocuments() {
    return new ArrayList<>(mergedDocuments.values());
  }

  void merge(JsonNode document) {

    seenDocumentsCount++;

    var thisDocScore =
        switch (document.get(DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD)) {
          case null -> DocumentScores.EMPTY;
          case NumericNode numberNode -> DocumentScores.withVectorScore(numberNode.floatValue());
          default ->
              throw new IllegalStateException(
                  "%s document field is not a number for doc _id %s"
                      .formatted(
                          DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD,
                          document.get(DocumentConstants.Fields.DOC_ID)));
        };

    var documentId =
        switch (document.get(DocumentConstants.Fields.DOC_ID)) {
          case null -> throw new IllegalArgumentException("Document id cannot be a null");
          case NullNode ignored ->
              throw new IllegalArgumentException("Document id cannot be a NullNode");
          case JsonNode jsonNode -> jsonNode;
        };

    var passage =
        switch (document.get(passageField)) {
          case null -> {
            // undefined in the document, default to empty string for passage
            yield null;
          }
          case TextNode textNode -> textNode.textValue();
          case NullNode ignored -> {
            // explicit {$vectorize : null} treat same as undefined, empty passage to rerank on
            yield null;
          }
          default ->
              throw new IllegalStateException(
                  "Passage field %s not a text node in document _id=%s"
                      .formatted(passageField, documentId));
        };

    LOGGER.debug("XXX merge() ID {} passage '{}'", documentId, Objects.toString(passage));
    if (passage == null || passage.isBlank()) {
      droppedDocumentsCount++;
      return;
    }

    // We ran a projection for the inner reads that was a combination of what the user asked for,
    // and
    // what we needed for the reranking (_id and reranking field), so we now need to run the users
    // projection on that raw document so they get what they asked for
    // NOTE:  this mutates the document
    userProjection.applyProjection(document);

    mergeScoredDocument(new ScoredDocument(documentId, document, passage, thisDocScore));
  }

  private void mergeScoredDocument(ScoredDocument scoredDocument) {

    var prevScoredDoc = mergedDocuments.putIfAbsent(scoredDocument.id(), scoredDocument);

    if (prevScoredDoc != null) {
      // we have already seen this document in the find results, merge the scores, it will detect
      // if a particular score is changed between the two results
      prevScoredDoc.mergeScore(scoredDocument.scores());
    }

    // It's a little messy, but for now we get the inner reads to build the $similarity score
    // into the doc, the user did not ask for it, so we should remove it
    if (scoredDocument.scores().vector().exists()) {
      scoredDocument.document().remove(DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD);
    }
  }
}
