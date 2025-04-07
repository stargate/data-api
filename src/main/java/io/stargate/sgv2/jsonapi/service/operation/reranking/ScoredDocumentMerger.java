package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScoredDocumentMerger {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScoredDocumentMerger.class);

  private final Map<JsonNode, ScoredDocument> mergedDocuments;
  private int seenDocumentsCount = 0;
  private int droppedDocumentsCount = 0;
  private final PathMatchLocator passageLocator;
  private final DocumentProjector userProjection;

  protected ScoredDocumentMerger(
      int initialCapacity, PathMatchLocator passageLocator, DocumentProjector userProjection) {
    mergedDocuments = new HashMap<>(initialCapacity);

    this.userProjection = userProjection;
    this.passageLocator = passageLocator;
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

  void merge(int rank, Rank.RankSource rankSource, JsonNode document) {

    seenDocumentsCount++;

    var scoredDocument = ScoredDocument.create(rank, rankSource, document, passageLocator);

    if (scoredDocument.passage().isEmpty()) {
      droppedDocumentsCount++;
      return;
    }

    // We ran a projection for the inner reads that was a combination of what the user asked for,
    // and
    // what we needed for the reranking (_id and reranking field), so we now need to run the users
    // projection on that raw document so they get what they asked for
    // NOTE:  this mutates the document
    userProjection.applyProjection(scoredDocument.document());

    // we will have one or the other of the vector or bm25 scores, merging handles this.
    mergeScoredDocument(scoredDocument);
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
