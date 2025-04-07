package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DOC_ID;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

/**
 * A document for scoring, with a passage and scores. The passage is used for reranking, the scores
 * are then used to sort the documents.
 *
 * <p>Create using {@link #create(int, Rank.RankSource, JsonNode, PathMatchLocator)} and then merge
 * the scores using {@link #mergeScore(DocumentScores)}.
 *
 * <p>TODO: more tests for the create function, only covering the passage extraction
 */
public class ScoredDocument implements Comparable<ScoredDocument> {

  // see compareTo()
  private static final Comparator<JsonNode> ID_NODE_COMPARATOR =
      Comparator.comparing(JsonNode::getNodeType).thenComparing(node -> node.asText());

  private final JsonNode id;
  private final ObjectNode document;
  private final Optional<String> passage;
  private DocumentScores scores;

  /** Package protected so we can test with explicit scores */
  @VisibleForTesting
  protected ScoredDocument(JsonNode id, JsonNode document, String passage, DocumentScores scores) {
    this(id, checkCast(document), passage, scores);
  }

  private static ObjectNode checkCast(JsonNode document) {
    if (document instanceof ObjectNode on) {
      return on;
    }
    Objects.requireNonNull(document, "document cannot be null");
    throw new IllegalArgumentException(
        "Expected document to be ObjectNode, got document.getNodeType()=%s"
            .formatted(document.getNodeType()));
  }

  private ScoredDocument(JsonNode id, ObjectNode document, String passage, DocumentScores scores) {

    this.id = Objects.requireNonNull(id);
    if (!id.isValueNode()) {
      // required for the compareTo method
      throw new IllegalArgumentException(
          "id must be a value node, got id.getNodeType()=%s".formatted(id.getNodeType()));
    }

    if (passage != null && passage.isBlank()) {
      throw new IllegalArgumentException("passage cannot be blank");
    }

    this.passage = Optional.ofNullable(passage);
    this.document = Objects.requireNonNull(document, "document cannot be null");
    this.scores = Objects.requireNonNull(scores, "scores cannot be null");
  }

  /**
   * Factory to create ane instance based on a document.
   *
   * @param rank Rank of this document, the order it appeared in the result set.
   * @param rankSource The source of the rank
   * @param document Document to extract the id and passage from.
   * @param passageLocator Locator to find the passage in the document.
   * @return A new instance of ScoredDocument.
   */
  static ScoredDocument create(
      int rank, Rank.RankSource rankSource, JsonNode document, PathMatchLocator passageLocator) {

    Objects.requireNonNull(document, "document cannot be null");
    Objects.requireNonNull(passageLocator, "passageLocator cannot be null");

    var documentId =
        switch (document.get(DOC_ID)) {
          case null -> throw new IllegalArgumentException("Document id cannot be missing");
          case NullNode ignored ->
              throw new IllegalArgumentException("Document id cannot be a NullNode");
          case JsonNode jsonNode -> jsonNode;
        };

    DocumentScores vectorScores;
    var similarityField = document.get(VECTOR_FUNCTION_SIMILARITY_FIELD);
    if (rankSource == Rank.RankSource.VECTOR) {
      if (similarityField instanceof NumericNode numberNode) {
        vectorScores = DocumentScores.fromVectorRead(numberNode.floatValue(), rank);
      } else {
        throw new IllegalArgumentException(
            "%s document field is not a number or is missing for doc _id %s"
                .formatted(VECTOR_FUNCTION_SIMILARITY_FIELD, documentId.asText()));
      }
    } else {
      if (similarityField != null) {
        throw new IllegalArgumentException(
            "rankSource is %s but the document with id '%s' has %s field=%s"
                .formatted(
                    rankSource, documentId, VECTOR_FUNCTION_SIMILARITY_FIELD, similarityField));
      }
      vectorScores = DocumentScores.EMPTY;
    }

    // if we dont have a vector score, then use the rank as the bm25 rank (it is one or the other)
    // if we have a vector score, then the BM25 is Empty
    var bm25Scores =
        rankSource == Rank.RankSource.BM25
            ? DocumentScores.fromBm25Read(rank)
            : DocumentScores.EMPTY;

    var passageNode = passageLocator.findValueIn(document);
    var passage =
        switch (passageNode) {
          case MissingNode ignored -> {
            // undefined in the document, default to null so it is dropped
            yield null;
          }
          case NullNode ignored -> {
            // explicit {$vectorize : null} treat same as undefined, empty passage to rerank on
            yield null;
          }
          case ValueNode valueNode -> {
            // could be text, number, boolean, but not array or object
            yield valueNode.asText();
          }
          default ->
              throw new IllegalArgumentException(
                  "Passage field %s is present but not null or a valueNode _id=%s , passageField=%s"
                      .formatted(passageLocator.path(), documentId, passageNode));
        };

    // we will have one or the other of the vector or bm25 scores, merging handles this.
    // normalise passage to null, to make it easier to use optional.
    // cannot rerank on a blank passage
    return new ScoredDocument(
        documentId,
        document,
        passage == null || passage.isBlank() ? null : passage,
        vectorScores.merge(bm25Scores));
  }

  public JsonNode id() {
    return id;
  }

  public Optional<String> passage() {
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
