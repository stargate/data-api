package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Comparator;
import java.util.Objects;

public class ScoredDocument implements Comparable<ScoredDocument> {

  // see compareTo()
  private static final Comparator<JsonNode> ID_NODE_COMPARATOR =
      Comparator.comparing(JsonNode::getNodeType).thenComparing(node -> node.asText());

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
    if (!id.isValueNode()) {
      // required for the compareTo method
      throw new IllegalArgumentException(
          "id must be a value node, got id.getNodeType()=%s".formatted(id.getNodeType()));
    }

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

  /**
   * Compares two scored documents.
   *
   * <p>Compares the scores using {@link DocumentScores#compareTo(DocumentScores)} if the scores are
   * equal we need something to sort on that is unique and stable. The text value of the id JsonNode
   * returns null for array and objects, text otherwise include numbers. So we check for array and
   * object types in the store, they are not allowed as _id in a document.
   *
   * <p>However, it is possible to have two different documents in a collection with the same
   * <b>text</b> representation for their _id. e.g. "100" as a text node and 100 as a number node So
   * we sort on both the {@link JsonNodeType} and then the text representation of the _id
   *
   * <p>This means documents are first sorted on the node type, in the order they are defined in
   * JsonNodeType and then on the id. So for example, all numbers occur before all text nodes, even
   * if they have the same text representation. See tests.
   *
   * @param other the object to be compared.
   * @return
   */
  @Override
  public int compareTo(ScoredDocument other) {
    var scoresCompare = scores.compareTo(other.scores);
    if (scoresCompare != 0) {
      return scoresCompare;
    }
    return ID_NODE_COMPARATOR.compare(this.id, other.id);
  }

  @Override
  public String toString() {
    return "ScoredDocument{"
        + "id="
        + id
        + ", passage='"
        + passage
        + '\''
        + ", document="
        + document
        + ", scores="
        + scores
        + '}';
  }
}
