package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class ScoredDocument implements Comparable<ScoredDocument> {

  private final JsonNode id;
  private final ObjectNode document;
  private DocumentScores scores;

  ScoredDocument(JsonNode id, JsonNode document, DocumentScores scores) {
    this(id, checkCast(document), scores);
  }
  private static ObjectNode checkCast(JsonNode document) {
    if (document instanceof ObjectNode on) {
      return on;
    }
    throw new IllegalArgumentException("Expected an ObjectNode, got document.getNodeType()=%s".formatted(document.getNodeType()));
  }

  ScoredDocument(JsonNode id, ObjectNode document, DocumentScores scores) {
    this.id = id;
    this.document = document;
    this.scores = scores;
  }

  public JsonNode id() {
    return id;
  }

  public ObjectNode document() {
    return document;
  }

  public DocumentScores scores() {
    return scores;
  }

  void mergeScore(DocumentScores other) {
    scores = scores.merge(other);
  }

  @Override
  public int compareTo(ScoredDocument o) {
    return scores.compareTo(o.scores);
  }


}
