package io.stargate.sgv2.jsonapi.service.operation.reranking;

import java.util.Objects;

/** */
public class DocumentScores implements Comparable<DocumentScores> {

  public static final DocumentScores EMPTY =
      new DocumentScores(Score.EMPTY_VECTOR, Score.EMPTY_RERANKING);

  private Score vectorScore;
  private Score rerankScore;

  private DocumentScores(Score vectorScore, Score rerankScore) {
    this.vectorScore = vectorScore;
    this.rerankScore = rerankScore;
  }

  static DocumentScores withVectorScore(float similarity) {
    return new DocumentScores(Score.vectorScore(similarity), Score.EMPTY_RERANKING);
  }

  static DocumentScores withRerankScore(float rerank) {
    return new DocumentScores(Score.EMPTY_VECTOR, Score.rerankScore(rerank));
  }

  public DocumentScores merge(DocumentScores other) {
    Objects.requireNonNull(other, "Other must not be null");
    return new DocumentScores(
        vectorScore.merge(other.vectorScore), rerankScore.merge(other.rerankScore));
  }

  /**
   * Returns the {@link DocumentScoresDesc} to describe the scores in the response of an API call
   */
  DocumentScoresDesc scoresDesc() {
    return new DocumentScoresDesc(vectorScore.score(), rerankScore.score());
  }

  Score rerank() {
    return rerankScore;
  }

  Score vector() {
    return vectorScore;
  }

  /**
   * Sorts based on the rerank score, other scores are ignored.
   *
   * <p>NOTE: this sorts in descending order, larger scores are better
   */
  @Override
  public int compareTo(DocumentScores other) {
    Objects.requireNonNull(other, "Other must not be null");
    return rerankScore.compareTo(other.rerankScore);
  }
}
