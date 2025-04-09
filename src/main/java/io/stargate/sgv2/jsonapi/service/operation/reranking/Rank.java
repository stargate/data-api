package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;

/**
 * An integer 1 based rank for a document / row in a result set. Ranks are sorted in ascending
 * order, smaller is better.
 *
 * <p>Similar to {@link Score} but this is a rank, not a score.
 *
 * <p>NOTE: does not implement {@link Comparable} because we do not sort them, has equals and hash
 * for testing
 */
public class Rank {

  @VisibleForTesting protected static final int NO_RANK = -1;

  public static final Rank EMPTY_VECTOR_RANK = new Rank(RankSource.VECTOR, false, NO_RANK);

  public static final Rank EMPTY_BM25_RANK = new Rank(RankSource.BM25, false, NO_RANK);

  public enum RankSource {
    /** Rank came from a vector similarity sort */
    VECTOR,
    /** Rank came from a BM25 sort */
    BM25;
  }

  private final RankSource source;
  private final boolean exists;
  private final int rank;

  private Rank(RankSource source, boolean exists, int rank) {
    this.source = source;
    this.exists = exists;
    this.rank = rank;

    if (exists && rank < 1) {
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

    Objects.requireNonNull(other, "other must not be null");
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

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Rank rank1)) {
      return false;
    }
    return exists == rank1.exists && rank == rank1.rank && source == rank1.source;
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, exists, rank);
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("Rank{")
        .append("source=")
        .append(source)
        .append(", exists=")
        .append(exists)
        .append(", rank=")
        .append(rank)
        .append('}')
        .toString();
  }
}
