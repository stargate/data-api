package io.stargate.sgv2.jsonapi.service.operation.reranking;

/**
 * An integer rank for a document / row in a result set. Ranks are sorted in ascending order,
 * smaller is better.
 */
public class Rank {

  private static final int NO_RANK = -1;

  public static final Rank EMPTY_VECTOR_RANK =
      new Rank(RankSource.VECTOR, false, NO_RANK);
  public static final Rank EMPTY_BM25_RANK = new Rank(RankSource.BM25, false, NO_RANK);

  public enum RankSource {
    VECTOR,
    BM25;
  }

  private final RankSource source;
  private final boolean exists;
  private final int rank;

  private Rank(RankSource source, boolean exists, int rank) {
    this.source = source;
    this.exists = exists;
    this.rank = rank;

    if (exists && rank <1) {
      throw new IllegalArgumentException("Rank must be greater than 0, got %s".formatted(rank));
    }
  }

  public boolean exists() {
    return exists;
  }

  public int rank() {
    return rank;
  }

  public RankSource source() {
    return source;
  }

  public static Rank vectorRank(int rank) {
    return new Rank(RankSource.VECTOR, true, rank);
  }

  public static Rank bm25Rank(int rank) {
    return new Rank(RankSource.BM25, true, rank);
  }

  Rank merge(Rank other) {
    checkSameSource("merge()", other);
    // can only merge if one of the scores does not exist
    if (exists && other.exists) {
      throw new IllegalArgumentException(
          "Cannot merge two ranks that both exist, got %s and %s".formatted(this, other));
    }
    return new Rank(source, exists || other.exists, exists ? rank : other.rank);
  }

  protected void checkSameSource(String context, Rank other) {
    if (source != other.source) {
      throw new IllegalArgumentException(
          "Cannot %s ranks from different sources, got %s and %s"
              .formatted(context, source, other.source));
    }
  }
}
