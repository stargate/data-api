package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link ScoredDocument} class.
 *
 * <p>Relies on {@link DocumentScoresTests} for comparing the scores, in here only testing the
 * reranking score and what happens when the same
 */
public class ScoredDocumentTests {

  private static final float SIMILARITY_SCORE = 0.75f;

  private static final JsonNode ID_1 = JsonNodeFactory.instance.textNode("id1");
  private static final JsonNode ID_2 = JsonNodeFactory.instance.textNode("id2");
  private static final JsonNode ID_3 = JsonNodeFactory.instance.textNode("id3");

  private static final String PASSAGE = "test PASSAGE";
  private static final PathMatchLocator VECTORIZE_LOCATOR =
      PathMatchLocator.forPath(VECTOR_EMBEDDING_TEXT_FIELD);
  private static final JsonNode DOCUMENT = baseDocument();

  private static ObjectNode baseDocument() {
    return JsonNodeFactory.instance
        .objectNode()
        .put(VECTOR_EMBEDDING_TEXT_FIELD, PASSAGE)
        .put(VECTOR_FUNCTION_SIMILARITY_FIELD, SIMILARITY_SCORE)
        .set(DOC_ID, ID_1);
  }

  private static ScoredDocument scoredDocument(JsonNode id, DocumentScores scores) {
    return new ScoredDocument(id, DOCUMENT, PASSAGE, scores);
  }

  @Test
  public void testComparisonDiffReranking() {

    var scoredDocument1 = scoredDocument(ID_1, DocumentScores.fromReranking(10f));
    var scoredDocument2 = scoredDocument(ID_2, DocumentScores.fromReranking(5f));
    var scoredDocument3 = scoredDocument(ID_3, DocumentScores.fromReranking(-10f));

    ScoreTests.threewayScoreComparison(scoredDocument1, scoredDocument2, scoredDocument3);
  }

  @Test
  public void testComparisonSameRerankingTextId() {

    var scoredDocument1 = scoredDocument(ID_1, DocumentScores.fromReranking(-10f));
    var scoredDocument2 = scoredDocument(ID_2, DocumentScores.fromReranking(-10f));
    var scoredDocument3 = scoredDocument(ID_3, DocumentScores.fromReranking(-10f));

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

    var scoredDoc = scoredDocument(ID_1, DocumentScores.EMPTY);

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

    var scoredDocument = scoredDocument(ID_1, DocumentScores.EMPTY);

    assertThat(scoredDocument.toString())
        .isEqualTo(
            "ScoredDocument{id=%s, passage='%s', document=%s, scores=%s}"
                .formatted(ID_1, Optional.of(PASSAGE), DOCUMENT, DocumentScores.EMPTY));
  }

  @Test
  public void testProperties() {

    var scoredDocument = scoredDocument(ID_1, DocumentScores.EMPTY);

    assertThat(scoredDocument.id()).isEqualTo(ID_1);
    assertThat(scoredDocument.passage()).isEqualTo(Optional.of(PASSAGE));
    assertThat(scoredDocument.document()).isEqualTo(DOCUMENT);
    assertThat(scoredDocument.scores()).isEqualTo(DocumentScores.EMPTY);
  }

  @Test
  public void testCtor() {

    var nullPassage = new ScoredDocument(ID_1, DOCUMENT, null, DocumentScores.EMPTY);
    assertThat(nullPassage.passage()).as("passage isEmpty() when passage param is null").isEmpty();

    assertThatThrownBy(
            () ->
                new ScoredDocument(
                    ID_1, JsonNodeFactory.instance.arrayNode(), PASSAGE, DocumentScores.EMPTY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Expected document to be ObjectNode, got document.getNodeType()=ARRAY");

    assertThatThrownBy(
            () ->
                new ScoredDocument(
                    JsonNodeFactory.instance.objectNode(), DOCUMENT, PASSAGE, DocumentScores.EMPTY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("id must be a value node, got id.getNodeType()=OBJECT");

    assertThatThrownBy(() -> new ScoredDocument(ID_1, null, PASSAGE, DocumentScores.EMPTY))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("document cannot be null");
    assertThatThrownBy(() -> new ScoredDocument(ID_1, DOCUMENT, PASSAGE, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("scores cannot be null");
  }

  /**
   * testing the $similarity field being present or not, this happens when the scores are present or
   * not in the response
   */
  @Test
  public void testSimilarityFieldPresence() {

    var docWithSimilarity = baseDocument();
    var withSimilarity =
        ScoredDocument.create(2, Rank.RankSource.VECTOR, docWithSimilarity, VECTORIZE_LOCATOR);

    assertThat(withSimilarity.scores().vector().score())
        .as("vector score matches when similarity in document")
        .isEqualTo(SIMILARITY_SCORE);
    assertThat(withSimilarity.scores().vector().exists())
        .as("vector score exists when similarity in document")
        .isTrue();
    assertThat(withSimilarity.scores().vectorRank().rank())
        .as("vector rank matches when similarity in document")
        .isEqualTo(2);
    assertThat(withSimilarity.scores().vectorRank().exists())
        .as("vector rank exists when similarity in document")
        .isTrue();

    var docNoSimilarity = baseDocument();
    docNoSimilarity.remove(VECTOR_FUNCTION_SIMILARITY_FIELD);
    var noSimilarity =
        ScoredDocument.create(3, Rank.RankSource.VECTOR, docNoSimilarity, VECTORIZE_LOCATOR);

    assertThat(noSimilarity.scores().vector().exists())
        .as("vector score does not exist when similarity not in  document")
        .isFalse();
    assertThat(noSimilarity.scores().vectorRank().rank())
        .as("vector rank matches when similarity not in  document")
        .isEqualTo(3);
    assertThat(noSimilarity.scores().vectorRank().exists())
        .as("vector rank exists when similarity not in  document")
        .isTrue();

    var docWrongType = baseDocument();
    docWrongType.put(VECTOR_FUNCTION_SIMILARITY_FIELD, "wrong type");
    assertThatThrownBy(
            () -> ScoredDocument.create(4, Rank.RankSource.VECTOR, docWrongType, VECTORIZE_LOCATOR))
        .as("vector score is not a number when similarity in document")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("$similarity document field is not a number or is missing type=String _id=id1");

    assertThatThrownBy(
            () -> ScoredDocument.create(4, Rank.RankSource.BM25, DOCUMENT, VECTORIZE_LOCATOR))
        .as("vector score present when a BM25 ranking")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "rankSource is BM25 but the document has similarity score id=\"id1\" has $similarity=0.75");
  }

  /** Testing the various ways the passage is extracted */
  @Test
  public void testCreatePassageField() {

    var undefinedField =
        ScoredDocument.create(
            1, Rank.RankSource.VECTOR, DOCUMENT, PathMatchLocator.forPath("missing"));
    assertThat(undefinedField.passage())
        .as("passage missing when field does not exist in document")
        .isEmpty();

    var docWithNull = baseDocument().putNull(VECTOR_EMBEDDING_TEXT_FIELD);
    var nullField =
        ScoredDocument.create(1, Rank.RankSource.VECTOR, docWithNull, VECTORIZE_LOCATOR);
    assertThat(nullField.passage()).as("passage missing when field is null").isEmpty();

    var docWithEmpty = baseDocument().put(VECTOR_EMBEDDING_TEXT_FIELD, "");
    var emptyField =
        ScoredDocument.create(1, Rank.RankSource.VECTOR, docWithEmpty, VECTORIZE_LOCATOR);
    assertThat(emptyField.passage()).as("passage missing when field is empty string").isEmpty();

    var docWithBlank = baseDocument().put(VECTOR_EMBEDDING_TEXT_FIELD, " ");
    var blankField =
        ScoredDocument.create(1, Rank.RankSource.VECTOR, docWithBlank, VECTORIZE_LOCATOR);
    assertThat(blankField.passage())
        .as("passage missing when field is single space (blank)")
        .isEmpty();

    var textField = ScoredDocument.create(1, Rank.RankSource.VECTOR, DOCUMENT, VECTORIZE_LOCATOR);
    assertThat(textField.passage())
        .as("passage is present and correct when field is a string")
        .isPresent()
        .hasValue(PASSAGE);

    var docWithNumber = baseDocument().put(VECTOR_EMBEDDING_TEXT_FIELD, 1);
    var numberField =
        ScoredDocument.create(1, Rank.RankSource.VECTOR, docWithNumber, VECTORIZE_LOCATOR);
    assertThat(numberField.passage())
        .as("passage is present and correct when field is a number")
        .isPresent()
        .hasValue("1");

    var docWithBoolean = baseDocument().put(VECTOR_EMBEDDING_TEXT_FIELD, true);
    var booleanField =
        ScoredDocument.create(1, Rank.RankSource.VECTOR, docWithBoolean, VECTORIZE_LOCATOR);
    assertThat(booleanField.passage())
        .as("passage is present and correct when field is a boolean")
        .isPresent()
        .hasValue("true");

    var docWithObjectPassage = baseDocument();
    docWithObjectPassage.putObject(VECTOR_EMBEDDING_TEXT_FIELD);
    assertThatThrownBy(
            () ->
                ScoredDocument.create(
                    1, Rank.RankSource.VECTOR, docWithObjectPassage, VECTORIZE_LOCATOR))
        .as("Cannot use object as passage node")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Passage field %s is present but not null or a valueNode _id=%s , passageField=%s"
                .formatted(
                    VECTOR_EMBEDDING_TEXT_FIELD,
                    ID_1,
                    docWithObjectPassage.get(VECTOR_EMBEDDING_TEXT_FIELD)));

    var docWithArrayPassage = baseDocument();
    docWithArrayPassage.putArray(VECTOR_EMBEDDING_TEXT_FIELD);
    assertThatThrownBy(
            () ->
                ScoredDocument.create(
                    1, Rank.RankSource.VECTOR, docWithArrayPassage, VECTORIZE_LOCATOR))
        .as("Cannot use array as passage node")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Passage field %s is present but not null or a valueNode _id=%s , passageField=%s"
                .formatted(
                    VECTOR_EMBEDDING_TEXT_FIELD,
                    ID_1,
                    docWithArrayPassage.get(VECTOR_EMBEDDING_TEXT_FIELD)));
  }
}
