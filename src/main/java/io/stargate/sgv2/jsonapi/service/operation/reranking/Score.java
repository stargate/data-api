package io.stargate.sgv2.jsonapi.service.operation.reranking;

import java.util.Objects;

/**
 * A floating point score, that may nor may not have a value and can be merged with another score of
 * the same source. And sorts in DESCENDING order, larger scores are better.
 *
 * <p>We need to merge scores because there are multiple places and times when a document is scored,
 * and we normally only have 1 or 2 of the difference scores at ach time. See {@link #merge(Score)}.
 *
 * <p>Create instances using the static constructors, merge them together, and then compare.
 */
public class Score implements Comparable<Score> {

  public static final Score EMPTY_RERANK_SCORE =
      new Score(ScoreSource.RERANKING, false, Integer.MIN_VALUE);

  public static final Score EMPTY_VECTOR_SCORE =
      new Score(ScoreSource.VECTOR, false, Integer.MIN_VALUE);

  public enum ScoreSource {
    /** Score came from a Reranking model */
    RERANKING,
    /** Score came from a vector similarity */
    VECTOR,
    /**
     * Score calculated by the Reciprocal Rank Fusion (RRF) algorithm Only supported by the {@link
     * RRFScore} class
     */
    RRF;
  }

  protected final ScoreSource source;
  protected final boolean exists;
  protected float score;

  private Score(ScoreSource source, boolean exists, float score) {
    // we cannot do any checking that the score is a certain value if exists or not
    // the scores are totally opaque, other than they must not be +/- NaN or inifinity
    if (!Float.isFinite(score)) {
      throw new IllegalArgumentException("Score must be finite, got %s".formatted(score));
    }

    this.source = source;
    this.exists = exists;
    this.score = score;
  }

  public float score() {
    return score;
  }

  public boolean exists() {
    return exists;
  }

  public ScoreSource source() {
    return source;
  }

  /** Creates a new score of with source {@link ScoreSource#VECTOR} */
  public static Score vectorScore(float score) {
    return new Score(ScoreSource.VECTOR, true, score);
  }

  /** Creates a new score with source {@link ScoreSource#VECTOR} */
  public static Score rerankScore(float rerank) {
    return new Score(ScoreSource.RERANKING, true, rerank);
  }

  /**
   * Merges this score with another, if they are both from the same source and none or one of them
   * exists.
   *
   * <p>NOTE: it is different for {@link RRFScore#merge(Score)}
   *
   * @param other The other score to merge with, must be from the same source
   * @return A new score that is the result of the merge, if one of the scores exists it's score
   *     will be set in the return. Otherwise, the score will be the same for an empty score.
   */
  Score merge(Score other) {

    Objects.requireNonNull(other, "other score cannot be null");
    checkSameSource("merge()", other);

    // can only merge if one of the scores does not exist
    if (exists && other.exists) {
      throw new IllegalArgumentException(
          "Cannot merge two scores that both exist, got %s and %s".formatted(this, other));
    }
    return new Score(source, exists || other.exists, exists ? score : other.score);
  }

  protected void checkSameSource(String context, Score other) {
    if (source != other.source) {
      throw new IllegalArgumentException(
          "Cannot %s scores from different sources, got %s and %s"
              .formatted(context, source, other.source));
    }
  }

  /**
   * Sorts based on the score
   *
   * <p>NOTE: this sorts in <b>descending</b> order, larger scores are better
   */
  @Override
  public int compareTo(Score other) {
    if (other == null) {
      return 1;
    }
    checkSameSource("compareTo()", other);

    return Float.compare(score, other.score) * -1;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("Score{")
        .append("source=")
        .append(source)
        .append(", exists=")
        .append(exists)
        .append(", score=")
        .append(score)
        .append('}')
        .toString();
  }

  // implement hash and equals
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Score score1)) {
      return false;
    }
    return exists == score1.exists
        && Float.compare(score1.score, score) == 0
        && source == score1.source;
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, exists, score);
  }

  /** A score that is calculated by the Reciprocal Rank Fusion (RRF) algorithm. */
  public static class RRFScore extends Score {
    private static final int K = 60; // see Reciprocal rank fusion online

    public static final RRFScore EMPTY_RRF = new RRFScore(false, 0);

    private RRFScore(boolean exists, float score) {
      super(ScoreSource.RRF, exists, score);
    }

    public static RRFScore create(int rank) {
      if (rank < 1) {
        throw new IllegalArgumentException("Rank must be >= 1, got %s".formatted(rank));
      }
      return new RRFScore(true, (float) (1.0 / (K + rank)));
    }

    /**
     * When we merge the RRF score we add them together, rather than replace.
     *
     * @param other
     * @return
     */
    @Override
    RRFScore merge(Score other) {
      checkSameSource("merge()", other);
      // does not matter if the scores exist, because the score is 0 if it does not exist
      // we add the scores together when merging RRF, this is part of the RRF algorithm
      // i.e. we have the RRF for the row from the ANN sort, and want to add the RRF from the BM25
      // sort
      return new RRFScore(exists || other.exists, score + other.score);
    }
  }
}
