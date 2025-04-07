package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link DocumentScores} class.
 *
 * <p>Note: the testing for {@link Score} is relied on, we are just checking the DocumentScore
 * compares the correct things.
 */
public class DocumentScoresTests {

  @Test
  public void testComparisonReranking() {
    var score1 = DocumentScores.fromReranking(10.0f);
    var score2 = DocumentScores.fromReranking(5.5f);
    var score3 = DocumentScores.fromReranking(-1.2f);

    ScoreTests.threewayScoreComparison(score1, score2, score3);
  }

  @Test
  public void testComparisonSameRerankingWithVectorOnly() {
    // duplicate scores tend to be negative
    // testing what happens with duplicates and only a vector read
    // note only the RANK of the vector is used with the RRF
    var score1 =
        DocumentScores.fromReranking(-18.765f).merge(DocumentScores.fromVectorRead(0.1f, 1));
    var score2 =
        DocumentScores.fromReranking(-18.765f).merge(DocumentScores.fromVectorRead(0.01f, 5));
    var score3 =
        DocumentScores.fromReranking(-18.765f).merge(DocumentScores.fromVectorRead(0.01f, 10));

    ScoreTests.threewayScoreComparison(score1, score2, score3);
  }

  @Test
  public void testComparisonSameRerankingWithBM25Only() {
    // duplicate scores tend to be negative
    // testing what happens with duplicates and only a BM25 read
    var score1 = DocumentScores.fromReranking(-18.765f).merge(DocumentScores.fromBm25Read(1));
    var score2 = DocumentScores.fromReranking(-18.765f).merge(DocumentScores.fromBm25Read(5));
    var score3 = DocumentScores.fromReranking(-18.765f).merge(DocumentScores.fromBm25Read(10));

    ScoreTests.threewayScoreComparison(score1, score2, score3);
  }

  @Test
  public void testComparisonSameRerankingWithBoth() {
    // duplicate scores tend to be negative
    // testing what happens with duplicates reads from both
    // see ScoresTests for the first 10 RRF scores

    // rrf 0.032522474
    var score1 =
        DocumentScores.fromReranking(-18.765f)
            .merge(DocumentScores.fromBm25Read(2))
            .merge(DocumentScores.fromVectorRead(0.1f, 1));

    // rrf 0.03076923
    var score2 =
        DocumentScores.fromReranking(-18.765f)
            .merge(DocumentScores.fromBm25Read(5))
            .merge(DocumentScores.fromVectorRead(0.1f, 5));

    // rrf 0.030679156
    var score3 =
        DocumentScores.fromReranking(-18.765f)
            .merge(DocumentScores.fromBm25Read(1))
            .merge(DocumentScores.fromVectorRead(0.1f, 10));

    ScoreTests.threewayScoreComparison(score1, score2, score3);
  }

  @Test
  public void testDescription() {
    var vectorRead = DocumentScores.fromVectorRead(1.0f, 1);
    var bm25Read = DocumentScores.fromBm25Read(2);
    var rerank = DocumentScores.fromReranking(1.0f);

    // RRF is additive
    var mergedRRF = vectorRead.rrf().merge(bm25Read.rrf()).merge(rerank.rrf());
    var allMerged = vectorRead.merge(bm25Read).merge(rerank);

    var desc = allMerged.scoresDesc();
    assertThat(desc.scores().vector())
        .as("description has vector score")
        .isEqualTo(vectorRead.vector().score());
    assertThat(desc.scores().vectorRank())
        .as("description has vector rank")
        .isEqualTo(vectorRead.vectorRank().rank());
    assertThat(desc.scores().rrf()).as("description has rrf").isEqualTo(mergedRRF.score());
    assertThat(desc.scores().rerank())
        .as("description has rerank")
        .isEqualTo(rerank.rerank().score());
    assertThat(desc.scores().bm25Rank())
        .as("description has bm25 rank")
        .isEqualTo(bm25Read.bm25Rank().rank());

    // null when no ranking from the bm25 or vector reads
    assertThat(rerank.scoresDesc().scores().vectorRank())
        .as("description has null vector rank")
        .isNull();
    assertThat(rerank.scoresDesc().scores().bm25Rank())
        .as("description has null bm25 rank")
        .isNull();
  }

  @Test
  public void testMerging() {
    var vectorRead = DocumentScores.fromVectorRead(1.0f, 1);
    var bm25Read = DocumentScores.fromBm25Read(2);
    var rerank = DocumentScores.fromReranking(1.0f);

    // RRF is additive
    var mergedRRF = vectorRead.rrf().merge(bm25Read.rrf()).merge(rerank.rrf());
    var allMerged = vectorRead.merge(bm25Read).merge(rerank);

    assertThat(allMerged.vector())
        .as("all merged has vector score")
        .isEqualTo(Score.vectorScore(1.0f));
    assertThat(allMerged.vectorRank())
        .as("all merged has vector rank")
        .isEqualTo(Rank.vectorRank(1));
    assertThat(allMerged.rrf()).as("all merged has rrf").isEqualTo(mergedRRF);
    assertThat(allMerged.rerank()).as("all merged has rerank").isEqualTo(Score.rerankScore(1.0f));
    assertThat(allMerged.bm25Rank()).as("all merged has bm25 rank").isEqualTo(Rank.bm25Rank(2));
  }

  @Test
  public void testFactories() {
    var vectorRead = DocumentScores.fromVectorRead(1.0f, 1);
    var bm25Read = DocumentScores.fromBm25Read(2);
    var rerank = DocumentScores.fromReranking(1.0f);

    assertThat(vectorRead.vector())
        .as("vector score has similarity")
        .isEqualTo(Score.vectorScore(1.0f));
    assertThat(vectorRead.vectorRank())
        .as("vector score has vector rank")
        .isEqualTo(Rank.vectorRank(1));
    assertThat(vectorRead.rrf()).as("vector score has rrf").isEqualTo(Score.RRFScore.create(1));
    assertThat(vectorRead.rerank())
        .as("vector score has empty rerank")
        .isEqualTo(Score.EMPTY_RERANK_SCORE);
    assertThat(vectorRead.bm25Rank())
        .as("vector score has empty bm25 rank")
        .isEqualTo(Rank.EMPTY_BM25_RANK);

    assertThat(bm25Read.vector())
        .as("bm25 score has empty vector score")
        .isEqualTo(Score.EMPTY_VECTOR_SCORE);
    assertThat(bm25Read.vectorRank())
        .as("bm25 score has empty vector rank")
        .isEqualTo(Rank.EMPTY_VECTOR_RANK);
    assertThat(bm25Read.rrf()).as("bm25 score has rrf").isEqualTo(Score.RRFScore.create(2));
    assertThat(bm25Read.rerank())
        .as("bm25 score has empty rerank")
        .isEqualTo(Score.EMPTY_RERANK_SCORE);
    assertThat(bm25Read.bm25Rank()).as("bm25 score has bm25 rank").isEqualTo(Rank.bm25Rank(2));

    assertThat(rerank.vector())
        .as("rerank score has empty vector score")
        .isEqualTo(Score.EMPTY_VECTOR_SCORE);
    assertThat(rerank.vectorRank())
        .as("rerank score has empty vector rank")
        .isEqualTo(Rank.EMPTY_VECTOR_RANK);
    assertThat(rerank.rrf()).as("rerank score has empty rrf").isEqualTo(Score.RRFScore.EMPTY_RRF);
    assertThat(rerank.rerank()).as("rerank score has rerank").isEqualTo(Score.rerankScore(1.0f));
    assertThat(rerank.bm25Rank())
        .as("rerank score has empty bm25 rank")
        .isEqualTo(Rank.EMPTY_BM25_RANK);
  }

  @Test
  public void testToString() {
    var vectorRead = DocumentScores.fromVectorRead(1.0f, 1);
    var bm25Read = DocumentScores.fromBm25Read(2);
    var rerank = DocumentScores.fromReranking(1.0f);

    // RRF is additive
    var mergedRRF = vectorRead.rrf().merge(bm25Read.rrf()).merge(rerank.rrf());
    var allMerged = vectorRead.merge(bm25Read).merge(rerank);

    assertThat(allMerged.toString())
        .as("toString")
        .contains("DocumentScores{")
        .contains("vectorScore=" + vectorRead.vector().toString())
        .contains("vectorRank=" + vectorRead.vectorRank().toString())
        .contains("rerankScore=" + rerank.rerank().toString())
        .contains("bm25Rank=" + bm25Read.bm25Rank().toString())
        .contains("rrfScore=" + mergedRRF.toString());
  }
}
