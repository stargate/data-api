package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

/** Tests for the {@link Score} class and {@link Score.RRFScore} class. */
public class ScoreTests {

  /**
   * Tests the comparison of three scores, that should be in the order 1,2,3 of the parameters.
   *
   * <p>NOTE: Higher scores are better, so the order is reversed
   *
   * <p>Protected so other scoring tests can use it
   */
  protected static <T extends Comparable<? super T>> void threewayScoreComparison(
      T score1, T score2, T score3) {

    assertThat(score1).as("score1 < score2").isLessThan(score2);
    assertThat(score2).as("score2 > score1").isGreaterThan(score1);
    assertThat(score2).as("score2 < score3").isLessThan(score3);
    assertThat(score3).as("score3 > score2").isGreaterThan(score2);
    assertThat(score1).as("score1 < score3").isLessThan(score3);
    assertThat(score3).as("score3 > score1").isGreaterThan(score1);
  }

  @Test
  public void testComparison() {
    // remember, this is a score where higher is better so the order is around the other way
    var rerank1 = Score.rerankScore(10.0f);
    var rerank2 = Score.rerankScore(5.0f);
    var rerank3 = Score.rerankScore(-11f);

    var vector1 = Score.vectorScore(1.0f);
    threewayScoreComparison(rerank1, rerank2, rerank3);

    assertThatThrownBy(() -> rerank1.compareTo(vector1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot compareTo() scores from different sources, got RERANKING and VECTOR");
  }

  @Test
  public void testMerging() {
    var rerank1 = Score.rerankScore(1.0f);
    var rerank2 = Score.rerankScore(0.5f);
    var vector1 = Score.vectorScore(1.0f);

    var rrfScore1 = Score.RRFScore.create(1);
    var rrfScore2 = Score.RRFScore.create(5);

    assertThat(rerank1.merge(Score.EMPTY_RERANK_SCORE))
        .as("merging score with empty")
        .isEqualTo(rerank1);
    assertThat(Score.EMPTY_RERANK_SCORE.merge(rerank1))
        .as("merging empty with score")
        .isEqualTo(rerank1);
    assertThat(Score.EMPTY_RERANK_SCORE.merge(rerank1))
        .as("merged is existing")
        .satisfies(Score::exists);

    assertThatThrownBy(() -> rerank1.merge(vector1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot merge() scores from different sources, got RERANKING and VECTOR");

    assertThatThrownBy(() -> rerank1.merge(rerank2))
        .as("cannot merge two existing scores")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot merge two scores that both exist");

    // merging for RRF is addition
    // and it can work with two existing scores
    assertThat(rrfScore1.merge(rrfScore2))
        .as("merging RRF scores")
        .satisfies(
            s -> {
              assertThat(s).as("merged is existing").satisfies(Score::exists);
              assertThat(s.score())
                  .as("merged score")
                  .isEqualTo(rrfScore1.score() + rrfScore2.score());
            });

    assertThat(rrfScore1.merge(Score.RRFScore.EMPTY_RRF))
        .as("merging RRF scores")
        .satisfies(
            s -> {
              assertThat(s).as("merged is existing").satisfies(Score::exists);
              assertThat(s.score()).as("merged score").isEqualTo(rrfScore1.score());
            });

    assertThatThrownBy(() -> rrfScore1.merge(vector1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot merge() scores from different sources, got RRF and VECTOR");
  }

  @Test
  public void testRRFResults() {
    // scores for rank 1 to 10
    float[] rrfScores = {
      0.016393442f, // 1 / 61
      0.016129032f, // 1 / 62
      0.015873016f, // 1 / 63
      0.015625000f, // 1 / 64
      0.015384615f, // 1 / 65
      0.015151516f, // 1 / 66
      0.014925373f, // 1 / 67
      0.014705882f, // 1 / 68
      0.014492754f, // 1 / 69
      0.014285714f // 1 / 70
    };

    for (int i = 1; i <= 10; i++) {
      var rrfScore = Score.RRFScore.create(i);
      assertThat(rrfScore.score())
          .as("RRF score for rank %d".formatted(i))
          .isEqualTo(rrfScores[i - 1]);
    }
  }

  @Test
  public void testToString() {
    var rerank = Score.rerankScore(1.0f);
    assertThat(rerank.toString())
        .as("toString")
        .isEqualTo("Score{source=RERANKING, exists=true, score=1.0}");

    var vector = Score.vectorScore(1.0f);
    assertThat(vector.toString())
        .as("toString")
        .isEqualTo("Score{source=VECTOR, exists=true, score=1.0}");
  }

  @Test
  public void testIllegalArguments() {
    assertThatThrownBy(() -> Score.rerankScore(Float.NaN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Score must be finite, got NaN");
    assertThatThrownBy(() -> Score.rerankScore(Float.POSITIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Score must be finite, got Infinity");
    assertThatThrownBy(() -> Score.rerankScore(Float.NEGATIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Score must be finite, got -Infinity");

    assertThatThrownBy(() -> Score.vectorScore(Float.NaN))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Score must be finite, got NaN");
    assertThatThrownBy(() -> Score.vectorScore(Float.POSITIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Score must be finite, got Infinity");
    assertThatThrownBy(() -> Score.vectorScore(Float.NEGATIVE_INFINITY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Score must be finite, got -Infinity");

    assertThatThrownBy(() -> Score.RRFScore.create(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Rank must be >= 1, got 0");
    assertThatThrownBy(() -> Score.RRFScore.create(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Rank must be >= 1, got -1");
  }

  @Test
  public void testFactoryValues() {

    var rerank = Score.rerankScore(1.0f);
    assertThat(rerank.source()).as("source").isEqualTo(Score.ScoreSource.RERANKING);
    assertThat(rerank.exists()).as("exists").isTrue();
    assertThat(rerank.score()).as("score").isEqualTo(1.0f);

    var vector = Score.vectorScore(1.0f);
    assertThat(vector.source()).as("source").isEqualTo(Score.ScoreSource.VECTOR);
    assertThat(vector.exists()).as("exists").isTrue();
    assertThat(vector.score()).as("score").isEqualTo(1.0f);

    var emptyRerank = Score.EMPTY_RERANK_SCORE;
    assertThat(emptyRerank.source()).as("source").isEqualTo(Score.ScoreSource.RERANKING);
    assertThat(emptyRerank.exists()).as("exists").isFalse();
    assertThat(emptyRerank.score()).as("score").isEqualTo(Integer.MIN_VALUE);

    var emptyVector = Score.EMPTY_VECTOR_SCORE;
    assertThat(emptyVector.source()).as("source").isEqualTo(Score.ScoreSource.VECTOR);
    assertThat(emptyVector.exists()).as("exists").isFalse();
    assertThat(emptyVector.score()).as("score").isEqualTo(Integer.MIN_VALUE);

    var rrfScore = Score.RRFScore.create(1);
    assertThat(rrfScore.source()).as("source").isEqualTo(Score.ScoreSource.RRF);
    assertThat(rrfScore.exists()).as("exists").isTrue();
    assertThat(rrfScore.score()).as("score").isEqualTo(0.016393442f);
  }

  @Test
  public void testEqualsAndHash() {
    var rerank1 = Score.rerankScore(1.0f);
    var rerank1a = Score.rerankScore(1.0f);
    var rerank2 = Score.rerankScore(0.5f);
    var vector1 = Score.vectorScore(1.0f);

    assertThat(rerank1).as("Object equals self").isEqualTo(rerank1);
    assertThat(rerank1).as("Object equals other").isEqualTo(rerank1a);

    assertThat(rerank1).as("Object not equals different type").isNotEqualTo("string");
    assertThat(rerank1).as("Object not equals different source").isNotEqualTo(vector1);
    assertThat(rerank1)
        .as("Object not equals different exists")
        .isNotEqualTo(Score.EMPTY_RERANK_SCORE);
    assertThat(rerank1).as("Object not equals different score").isNotEqualTo(rerank2);

    assertThat(rerank1.hashCode()).as("hash code equals self").isEqualTo(rerank1.hashCode());
    assertThat(rerank1.hashCode()).as("hash code equals other").isEqualTo(rerank1a.hashCode());

    assertThat(rerank1.hashCode())
        .as("hash code different source")
        .isNotEqualTo(vector1.hashCode());
    assertThat(rerank1.hashCode())
        .as("hash code different exists")
        .isNotEqualTo(Score.EMPTY_RERANK_SCORE);
    assertThat(rerank1.hashCode()).as("hash code different score").isNotEqualTo(rerank2.hashCode());
  }
}
