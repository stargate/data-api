package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ScoredDocumentMerger {

  private final Map<JsonNode, ScoredDocument> mergedDocuments;
  private int seenDocumentsCount = 0;
  private final boolean includeSimilarityScore;

  ScoredDocumentMerger(int initialCapacity, boolean includeSimilarityScore) {
    mergedDocuments = new HashMap<>(initialCapacity);
    this.includeSimilarityScore = includeSimilarityScore;
  }

  int seenDocuments() {
    return seenDocumentsCount;
  }

  List<ScoredDocument> mergedDocuments(){
    return new ArrayList<>(mergedDocuments.values());
  }

  void mergeDocument(ScoredDocument scoredDocument){

    seenDocumentsCount++;
    var prevScoredDoc = mergedDocuments.putIfAbsent(scoredDocument.id(), scoredDocument);

    if (prevScoredDoc != null) {
      // we have already seen this document in the find results, merge the scores, it will detect
      // if a particular score is changed between the two results
      prevScoredDoc.mergeScore(scoredDocument.scores());

      // This is a little messy, we need to make sure the vector score is in the document we are going
      // to keep. If the user asked for it to be there, it may be in this doc and not the one we are keeping.
      if (scoredDocument.scores().vector().exists() && includeSimilarityScore) {
        prevScoredDoc.document().put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, scoredDocument.scores().vector().score());
      }
      else {
        // if the user did not ask for the vector score, we need to remove it from the document
        // we are going to keep
        if (scoredDocument.scores().vector().exists() && !includeSimilarityScore) {
          scoredDocument.document().remove(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
        }
      }
        }
      }


}
