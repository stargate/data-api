package io.stargate.sgv2.jsonapi.service.operation.reranking;

public class Score implements Comparable<Score> {

  public static final Score EMPTY_RERANKING = new Score(ScoreSource.RERANKING, false, Integer.MIN_VALUE);
  public static final Score EMPTY_VECTOR = new Score(ScoreSource.VECTOR, false, Integer.MIN_VALUE);

  public enum ScoreSource{
    RERANKING,
    VECTOR;
  }

  private final ScoreSource source;
  private final boolean exists;
  private float score;

  private Score(ScoreSource source, boolean exists, float score) {
    // we cannot do any checking that the score is a certain value if exists or not
    // the scores are totally opaque to us
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

  public ScoreSource source(){
    return source;
  }

  public static Score vectorScore(float score) {
    return new Score(ScoreSource.VECTOR, true, score);
  }

  public static Score rerankScore(float rerank) {
    return new Score(ScoreSource.RERANKING, true, rerank);
  }

  Score merge(Score other) {
    checkSameSource("merge()", other);
    // can only merge if one of the scores does not exist
    if (exists && other.exists) {
      throw new IllegalArgumentException("Cannot merge two scores that both exist, got %s and %s".formatted(this, other));
    }
    return new Score(source, exists || other.exists, exists ? score : other.score);
  }

  private void checkSameSource(String context, Score other){
    if (source != other.source) {
      throw new IllegalArgumentException("Cannot %s scores from different sources, got %s and %s".formatted(context, source, other.source));
    }
  }


  /**
   * Sorts based on the score
   *
   * <p>NOTE: this sorts in <b>descending</b> order, larger scores are better
   */
  @Override
  public int compareTo( Score other) {
    if (other == null) {
      return 1;
    }
    checkSameSource("compareTo()", other);

    return Float.compare(score, other.score) * -1;
  }
}
