package io.stargate.sgv2.jsonapi.service.operation.reranking;

import java.util.Objects;

/** */
public class DocumentScores implements Comparable<DocumentScores> {

  public static final DocumentScores EMPTY =
      new DocumentScores(
          Score.EMPTY_RERANK_SCORE,
          Score.EMPTY_VECTOR_SCORE,
          Rank.EMPTY_VECTOR_RANK,
          Rank.EMPTY_BM25_RANK,
          Score.RRFScore.EMPTY_RRF);

  private Score vectorScore;
  private Score rerankScore;
  private Score.RRFScore rrfScore;
  private Rank vectorRank;
  private Rank bm25Rank;

  private DocumentScores(
      Score rerankScore,
      Score vectorScore,
      Rank vectorRank,
      Rank bm25Rank,
      Score.RRFScore rrfScore) {
    this.rerankScore = rerankScore;
    this.vectorScore = vectorScore;
    this.vectorRank = vectorRank;
    this.bm25Rank = bm25Rank;
    this.rrfScore = rrfScore;
  }

  static DocumentScores fromBm25Read(int rank) {
    return new DocumentScores(
        Score.EMPTY_RERANK_SCORE,
        Score.EMPTY_VECTOR_SCORE,
        Rank.EMPTY_VECTOR_RANK,
        Rank.bm25Rank(rank),
        Score.RRFScore.create(rank));
  }

  static DocumentScores fromVectorRead(float similarity, int rank) {
    return new DocumentScores(
        Score.EMPTY_RERANK_SCORE,
        Score.vectorScore(similarity),
        Rank.vectorRank(rank),
        Rank.EMPTY_BM25_RANK,
        Score.RRFScore.create(rank));
  }

  static DocumentScores fromReranking(float rerank) {
    return new DocumentScores(
        Score.rerankScore(rerank),
        Score.EMPTY_VECTOR_SCORE,
        Rank.EMPTY_VECTOR_RANK,
        Rank.EMPTY_BM25_RANK,
        Score.RRFScore.EMPTY_RRF);
  }

  public DocumentScores merge(DocumentScores other) {
    Objects.requireNonNull(other, "Other must not be null");

    return new DocumentScores(
        rerankScore.merge(other.rerankScore),
        vectorScore.merge(other.vectorScore),
        vectorRank.merge(other.vectorRank),
        bm25Rank.merge(other.bm25Rank),
        rrfScore.merge(other.rrfScore));
  }

  /**
   * Returns the {@link DocumentScoresDesc} to describe the scores in the response of an API call
   */
  DocumentScoresDesc scoresDesc() {
    return new DocumentScoresDesc(
        rerankScore.score(),
        vectorScore.score(),
        vectorRank.exists() ?  vectorRank.rank() : null,
        bm25Rank.exists() ?  bm25Rank.rank() : null,
        rrfScore.score());
  }

  Score rerank() {
    return rerankScore;
  }

  Score vector() {
    return vectorScore;
  }

  Rank vectorRank() {
    return vectorRank;
  }

  Rank bm25Rank() {
    return bm25Rank;
  }

  Score.RRFScore rrf() {
    return rrfScore;
  }
  /**
   * Sorts based on the rerank score, other scores are ignored.
   *
   * <p>NOTE: this sorts in descending order, larger scores are better
   */
  @Override
  public int compareTo(DocumentScores other) {
    Objects.requireNonNull(other, "Other must not be null");
    var rerankCompare = rerankScore.compareTo(other.rerankScore);
    if (rerankCompare != 0) {
      return rerankCompare;
    }

    return rrfScore.compareTo(other.rrfScore);
  }
}
