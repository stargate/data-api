package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Tests for the {@link Rank} class. */
public class RankTests {

  @Test
  public void testMerging() {
    var vectorRank1 = Rank.vectorRank(1);
    var vectorRank2 = Rank.vectorRank(2);
    var bm25Rank1 = Rank.bm25Rank(1);

    assertThat(vectorRank1.merge(Rank.EMPTY_VECTOR_RANK))
        .as("merging rank with empty")
        .isEqualTo(vectorRank1);
    assertThat(Rank.EMPTY_VECTOR_RANK.merge(vectorRank1))
        .as("merging empty with rank")
        .isEqualTo(vectorRank1);
    assertThat(Rank.EMPTY_VECTOR_RANK.merge(vectorRank1))
        .as("merged is existing")
        .satisfies(Rank::exists);

    assertThatThrownBy(() -> vectorRank1.merge(bm25Rank1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot merge() ranks from different sources, got VECTOR and BM25");

    assertThatThrownBy(() -> vectorRank1.merge(vectorRank2))
        .as("cannot merge two existing ranks")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot merge two ranks that both exist");
  }

  @Test
  public void testToString() {
    var vectorRank = Rank.vectorRank(1);
    var bm25Rank = Rank.bm25Rank(1);
    var emptyVectorRank = Rank.EMPTY_VECTOR_RANK;
    var emptyBM25Rank = Rank.EMPTY_BM25_RANK;

    assertThat(vectorRank.toString())
        .as("vector rank toString")
        .isEqualTo("Rank{source=VECTOR, exists=true, rank=1}");
    assertThat(bm25Rank.toString())
        .as("BM25 rank toString")
        .isEqualTo("Rank{source=BM25, exists=true, rank=1}");
    assertThat(emptyVectorRank.toString())
        .as("empty vector rank toString")
        .isEqualTo("Rank{source=VECTOR, exists=false, rank=-1}");
    assertThat(emptyBM25Rank.toString())
        .as("empty BM25 rank toString")
        .isEqualTo("Rank{source=BM25, exists=false, rank=-1}");
  }

  @Test
  public void testIllegalArguments() {

    assertThatThrownBy(() -> Rank.vectorRank(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Rank must be greater than 0, got 0");
    assertThatThrownBy(() -> Rank.vectorRank(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Rank must be greater than 0, got -1");

    assertThatThrownBy(() -> Rank.bm25Rank(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Rank must be greater than 0, got 0");
    assertThatThrownBy(() -> Rank.bm25Rank(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Rank must be greater than 0, got -1");
  }

  @Test
  public void testFactoryValues() {

    var vectorRank = Rank.vectorRank(1);
    var bm25Rank = Rank.bm25Rank(1);
    var emptyVectorRank = Rank.EMPTY_VECTOR_RANK;
    var emptyBM25Rank = Rank.EMPTY_BM25_RANK;

    assertThat(vectorRank.exists()).as("vector rank exists").isTrue();
    assertThat(vectorRank.rank()).as("vector rank").isEqualTo(1);
    assertThat(vectorRank.source()).as("vector rank source").isEqualTo(Rank.RankSource.VECTOR);

    assertThat(bm25Rank.exists()).as("BM25 rank exists").isTrue();
    assertThat(bm25Rank.rank()).as("BM25 rank").isEqualTo(1);
    assertThat(bm25Rank.source()).as("BM25 rank source").isEqualTo(Rank.RankSource.BM25);

    assertThat(emptyVectorRank.exists()).as("empty vector rank exists").isFalse();
    assertThat(emptyVectorRank.rank()).as("empty vector rank").isEqualTo(Rank.NO_RANK);
    assertThat(emptyVectorRank.source())
        .as("empty vector rank source")
        .isEqualTo(Rank.RankSource.VECTOR);

    assertThat(emptyBM25Rank.exists()).as("empty BM25 rank exists").isFalse();
    assertThat(emptyBM25Rank.rank()).as("empty BM25 rank").isEqualTo(Rank.NO_RANK);
    assertThat(emptyBM25Rank.source()).as("empty BM25 rank source").isEqualTo(Rank.RankSource.BM25);
  }

  @Test
  public void testEqualsAndHash() {

    var vectorRank1 = Rank.vectorRank(1);
    var vectorRank1a = Rank.vectorRank(1);
    var vectorRank2 = Rank.vectorRank(2);
    var bm25Rank1 = Rank.bm25Rank(1);

    assertThat(vectorRank1).as("Object equals self").isEqualTo(vectorRank1);
    assertThat(vectorRank1).as("Object equals other").isEqualTo(vectorRank1a);

    assertThat(vectorRank1).as("Object not equals different type").isNotEqualTo("string");
    assertThat(vectorRank1).as("Object not equals different source").isNotEqualTo(bm25Rank1);
    assertThat(vectorRank1)
        .as("Object not equals different exists")
        .isNotEqualTo(Rank.EMPTY_VECTOR_RANK);
    assertThat(vectorRank1).as("Object not equals different rank").isNotEqualTo(vectorRank2);

    assertThat(vectorRank1.hashCode())
        .as("hash code equals self")
        .isEqualTo(vectorRank1.hashCode());
    assertThat(vectorRank1.hashCode())
        .as("hash code equals other")
        .isEqualTo(vectorRank1a.hashCode());

    assertThat(vectorRank1.hashCode())
        .as("hash code different source")
        .isNotEqualTo(bm25Rank1.hashCode());
    assertThat(vectorRank1.hashCode())
        .as("hash code different exists")
        .isNotEqualTo(Rank.EMPTY_VECTOR_RANK.hashCode());
    assertThat(vectorRank1.hashCode())
        .as("hash code different rank")
        .isNotEqualTo(vectorRank2.hashCode());
  }
}
