package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link ScoredDocument} class.
 *
 * <p>Relies on {@link DocumentScoresTests} for comparing the scores, in here only testing the
 * reranking score and what happens when the same
 */
public class ScoredDocumentTests {
  private static final String PASSAGE = "test PASSAGE";
  private static final JsonNode DOCUMENT =
      JsonNodeFactory.instance.objectNode().put("$vectorize", PASSAGE);

  private static ScoredDocument scoredDocument(JsonNode id, DocumentScores scores) {
    return new ScoredDocument(id, DOCUMENT, PASSAGE, scores);
  }

  @Test
  public void testComparisonDiffReranking() {

    var scoredDocument1 =
        scoredDocument(JsonNodeFactory.instance.textNode("id1"), DocumentScores.fromReranking(10f));
    var scoredDocument2 =
        scoredDocument(JsonNodeFactory.instance.textNode("id2"), DocumentScores.fromReranking(5f));
    var scoredDocument3 =
        scoredDocument(
            JsonNodeFactory.instance.textNode("id3"), DocumentScores.fromReranking(-10f));

    ScoreTests.threewayScoreComparison(scoredDocument1, scoredDocument2, scoredDocument3);
  }

  @Test
  public void testComparisonSameRerankingTextId() {

    var scoredDocument1 =
        scoredDocument(
            JsonNodeFactory.instance.textNode("id1"), DocumentScores.fromReranking(-10f));
    var scoredDocument2 =
        scoredDocument(
            JsonNodeFactory.instance.textNode("id2"), DocumentScores.fromReranking(-10f));
    var scoredDocument3 =
        scoredDocument(
            JsonNodeFactory.instance.textNode("id3"), DocumentScores.fromReranking(-10f));

    ScoreTests.threewayScoreComparison(scoredDocument1, scoredDocument2, scoredDocument3);
  }

  @Test
  public void testComparisonSameRerankingNumberId() {

    var scoredDocument1 =
        scoredDocument(
            JsonNodeFactory.instance.numberNode(100), DocumentScores.fromReranking(-10f));
    var scoredDocument2 =
        scoredDocument(
            JsonNodeFactory.instance.numberNode(200), DocumentScores.fromReranking(-10f));
    var scoredDocument3 =
        scoredDocument(
            JsonNodeFactory.instance.numberNode(300), DocumentScores.fromReranking(-10f));

    ScoreTests.threewayScoreComparison(scoredDocument1, scoredDocument2, scoredDocument3);
  }

  @Test
  public void testComparisonSameRerankingMixedId() {

    // per the ScoredDocument, it sorts on the type of the node first, and all numbers are before
    // text
    var scoredDocument1 =
        scoredDocument(
            JsonNodeFactory.instance.numberNode(100), DocumentScores.fromReranking(-10f));
    var scoredDocument2 =
        scoredDocument(
            JsonNodeFactory.instance.numberNode(200), DocumentScores.fromReranking(-10f));
    var scoredDocument3 =
        scoredDocument(JsonNodeFactory.instance.textNode("50"), DocumentScores.fromReranking(-10f));

    ScoreTests.threewayScoreComparison(scoredDocument1, scoredDocument2, scoredDocument3);
  }

  @Test
  public void testMerge() {
    // not testing all score merging, just that the scores changed

    var vectorRead = DocumentScores.fromVectorRead(1.0f, 1);
    var bm25Read = DocumentScores.fromBm25Read(2);
    var rerank = DocumentScores.fromReranking(1.0f);
    // RRF is additive
    var mergedRRF = vectorRead.rrf().merge(bm25Read.rrf()).merge(rerank.rrf());
    var allMerged = vectorRead.merge(bm25Read).merge(rerank);

    var scoredDoc = scoredDocument(JsonNodeFactory.instance.textNode("id1"), DocumentScores.EMPTY);

    assertThat(scoredDoc.scores()).as("initial scores empty").isEqualTo(DocumentScores.EMPTY);

    scoredDoc.mergeScore(allMerged);

    // there is no equals on the DocumentScores, so we need to check the individual scores
    assertThat(scoredDoc.scores().vector())
        .as("merged scoredoc has vector score")
        .isEqualTo(allMerged.vector());
    assertThat(scoredDoc.scores().vectorRank())
        .as("merged scoredoc has vector rank")
        .isEqualTo(allMerged.vectorRank());
    assertThat(scoredDoc.scores().rrf()).as("merged scoredoc has rrf").isEqualTo(mergedRRF);
    assertThat(scoredDoc.scores().rerank())
        .as("merged scoredoc has rerank")
        .isEqualTo(allMerged.rerank());
    assertThat(scoredDoc.scores().bm25Rank())
        .as("merged scoredoc has bm25 rank")
        .isEqualTo(allMerged.bm25Rank());
  }

  @Test
  public void testToString() {

    var id = JsonNodeFactory.instance.textNode("id1");
    var scoredDocument = scoredDocument(id, DocumentScores.EMPTY);

    assertThat(scoredDocument.toString())
        .isEqualTo(
            "ScoredDocument{id=%s, passage='%s', document=%s, scores=%s}"
                .formatted(id, PASSAGE, DOCUMENT, DocumentScores.EMPTY));
  }

  @Test
  public void testProperties() {

    var id = JsonNodeFactory.instance.textNode("id1");
    var scoredDocument = scoredDocument(id, DocumentScores.EMPTY);

    assertThat(scoredDocument.id()).isEqualTo(id);
    assertThat(scoredDocument.passage()).isEqualTo(PASSAGE);
    assertThat(scoredDocument.document()).isEqualTo(DOCUMENT);
    assertThat(scoredDocument.scores()).isEqualTo(DocumentScores.EMPTY);
  }

  @Test
  public void testCtor() {
    var id = JsonNodeFactory.instance.textNode("id1");

    assertThatThrownBy(
            () ->
                new ScoredDocument(
                    id, JsonNodeFactory.instance.arrayNode(), PASSAGE, DocumentScores.EMPTY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Expected an ObjectNode, got document.getNodeType()=ARRAY");

    assertThatThrownBy(
            () ->
                new ScoredDocument(
                    JsonNodeFactory.instance.objectNode(), DOCUMENT, PASSAGE, DocumentScores.EMPTY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("id must be a value node, got id.getNodeType()=OBJECT");

    assertThatThrownBy(() -> new ScoredDocument(id, null, PASSAGE, DocumentScores.EMPTY))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("document cannot be null");
    assertThatThrownBy(() -> new ScoredDocument(id, DOCUMENT, null, DocumentScores.EMPTY))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("passage cannot be null");
    assertThatThrownBy(() -> new ScoredDocument(id, DOCUMENT, PASSAGE, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("scores cannot be null");
  }
}
