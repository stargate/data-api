package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

class ScoredDocument implements Comparable<ScoredDocument> {

  private final JsonNode id;
  private final ObjectNode document;
  private final String passage;
  private DocumentScores scores;

  ScoredDocument(JsonNode id, JsonNode document, String passage, DocumentScores scores) {
    this(id, checkCast(document), passage, scores);
  }

  private static ObjectNode checkCast(JsonNode document) {
    if (document instanceof ObjectNode on) {
      return on;
    }
    throw new IllegalArgumentException(
        "Expected an ObjectNode, got document.getNodeType()=%s".formatted(document.getNodeType()));
  }

  ScoredDocument(JsonNode id, ObjectNode document, String passage, DocumentScores scores) {
    this.id = Objects.requireNonNull(id);
    this.passage = Objects.requireNonNull(passage, "passage cannot be null");
    this.document = Objects.requireNonNull(document, "document cannot be null");
    this.scores = Objects.requireNonNull(scores, "scores cannot be null");
  }

  public JsonNode id() {
    return id;
  }

  public String passage() {
    return passage;
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
