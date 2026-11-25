package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.FieldSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.LexicalSortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression2.VectorSortExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.reranking.Rank;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FindAndRerankPlan} and its records: the rules their constructors check, the rank
 * source of each read, how a {@link FindAndRerankPlan.VectorReadPlan} compares, and that the reads
 * of a plan and the sorts of a {@link FindAndRerankPlan.FilterReadPlan} cannot change.
 */
class FindAndRerankPlanTest {

  private static final float[] VECTOR = {0.1f, 0.2f};

  private static final FindAndRerankPlan.VectorReadPlan VECTOR_READ =
      new FindAndRerankPlan.VectorReadPlan(new VectorSortExpression(VECTOR), 50);

  private static final FindAndRerankPlan.VectorizeReadPlan VECTORIZE_READ =
      new FindAndRerankPlan.VectorizeReadPlan(50);

  private static final FindAndRerankPlan.LexicalReadPlan LEXICAL_READ =
      new FindAndRerankPlan.LexicalReadPlan(new LexicalSortExpression("cows"), 50);

  private static final List<FieldSortExpression> FIELD_SORTS =
      List.of(new FieldSortExpression("age", false), new FieldSortExpression("name", true));

  private static final FindAndRerankPlan.FilterReadPlan FILTER_READ =
      new FindAndRerankPlan.FilterReadPlan(FIELD_SORTS, 50);

  private static final FindAndRerankPlan.VectorizeStep VECTORIZE_STEP =
      new FindAndRerankPlan.VectorizeStep(
          "cheese", 3, new VectorizeDefinition("custom", "custom", null, null));

  private static final FindAndRerankPlan.RerankStep RERANK_STEP = rerankStep(10);

  // ===========================================================================================
  // FindAndRerankPlan
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidPlans")
  void rejectsInvalidPlan(
      String description,
      FindAndRerankPlan.VectorizeStep vectorizeStep,
      List<FindAndRerankPlan.ReadPlan> reads,
      String message) {
    assertThatThrownBy(() -> new FindAndRerankPlan(vectorizeStep, reads, RERANK_STEP, false, false))
        .as(description)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message);
  }

  static Stream<Arguments> invalidPlans() {
    return Stream.of(
        Arguments.of("reads empty", null, List.of(), "reads must not be empty"),
        Arguments.of(
            "two lexical reads",
            null,
            List.of(VECTOR_READ, LEXICAL_READ, LEXICAL_READ),
            "reads must have at most one read for each rank source, rankSources=[VECTOR, BM25, BM25]"),
        Arguments.of(
            "a vector read and a vectorize read, both have the VECTOR rank source",
            VECTORIZE_STEP,
            List.of(VECTOR_READ, VECTORIZE_READ),
            "reads must have at most one read for each rank source, rankSources=[VECTOR, VECTOR]"),
        Arguments.of(
            "vectorize read, no vectorize step",
            null,
            List.of(VECTORIZE_READ),
            "vectorizeStep must not be null when there is a VectorizeReadPlan"),
        Arguments.of(
            "vectorize step, no vectorize read",
            VECTORIZE_STEP,
            List.of(VECTOR_READ),
            "vectorizeStep must be null when there is no VectorizeReadPlan"),
        Arguments.of(
            "vectorize step, only a filter read",
            VECTORIZE_STEP,
            List.of(FILTER_READ),
            "vectorizeStep must be null when there is no VectorizeReadPlan"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("validPlans")
  void acceptsValidPlan(
      String description,
      FindAndRerankPlan.VectorizeStep vectorizeStep,
      List<FindAndRerankPlan.ReadPlan> reads) {
    assertThatCode(() -> new FindAndRerankPlan(vectorizeStep, reads, RERANK_STEP, false, false))
        .as(description)
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validPlans() {
    return Stream.of(
        Arguments.of("one lexical read, no vector read needed", null, List.of(LEXICAL_READ)),
        Arguments.of("one filter read, no vector read needed", null, List.of(FILTER_READ)),
        Arguments.of("vector read, no vectorize step", null, List.of(VECTOR_READ)),
        Arguments.of(
            "vectorize read with the vectorize step, lexical read",
            VECTORIZE_STEP,
            List.of(VECTORIZE_READ, LEXICAL_READ)));
  }

  @Test
  void copiesTheReads() {
    var reads = new ArrayList<FindAndRerankPlan.ReadPlan>(List.of(VECTOR_READ));
    var plan = new FindAndRerankPlan(null, reads, RERANK_STEP, false, false);

    reads.add(LEXICAL_READ);

    assertThat(plan.reads()).containsExactly(VECTOR_READ);
  }

  @Test
  void readsCannotBeChanged() {
    // a list that can be changed, so only the plan's own copy makes the reads unmodifiable
    var plan =
        new FindAndRerankPlan(
            null,
            new ArrayList<FindAndRerankPlan.ReadPlan>(List.of(VECTOR_READ)),
            RERANK_STEP,
            false,
            false);

    assertThatThrownBy(() -> plan.reads().add(LEXICAL_READ))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ===========================================================================================
  // ReadPlan records
  // ===========================================================================================

  @Test
  void rejectsVectorReadWithoutSort() {
    assertThatThrownBy(() -> new FindAndRerankPlan.VectorReadPlan(null, 50))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("sort must not be null");
  }

  @Test
  void rejectsLexicalReadWithoutSort() {
    assertThatThrownBy(() -> new FindAndRerankPlan.LexicalReadPlan(null, 50))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("sort must not be null");
  }

  @Test
  void rejectsFilterReadWithNullSorts() {
    assertThatThrownBy(() -> new FindAndRerankPlan.FilterReadPlan(null, 50))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("sorts must not be null");
  }

  /** A command with no sort reads with a filter read that has no sorts. */
  @Test
  void acceptsFilterReadWithNoSorts() {
    assertThat(new FindAndRerankPlan.FilterReadPlan(List.of(), 50).sorts()).isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("readsWithReadLimit")
  void rejectsReadLimitBelowOne(String description, IntFunction<FindAndRerankPlan.ReadPlan> read) {
    assertThatThrownBy(() -> read.apply(0))
        .as(description)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("readLimit must be at least 1, readLimit=0");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("readsWithReadLimit")
  void acceptsReadLimitOfOne(String description, IntFunction<FindAndRerankPlan.ReadPlan> read) {
    assertThat(read.apply(1).readLimit()).as(description).isEqualTo(1);
  }

  /** Makes a read of each kind with the read limit. */
  static Stream<Arguments> readsWithReadLimit() {
    return Stream.of(
        Arguments.of(
            "vector read",
            (IntFunction<FindAndRerankPlan.ReadPlan>)
                readLimit ->
                    new FindAndRerankPlan.VectorReadPlan(
                        new VectorSortExpression(VECTOR), readLimit)),
        Arguments.of(
            "vectorize read",
            (IntFunction<FindAndRerankPlan.ReadPlan>) FindAndRerankPlan.VectorizeReadPlan::new),
        Arguments.of(
            "lexical read",
            (IntFunction<FindAndRerankPlan.ReadPlan>)
                readLimit ->
                    new FindAndRerankPlan.LexicalReadPlan(
                        new LexicalSortExpression("cows"), readLimit)),
        Arguments.of(
            "filter read",
            (IntFunction<FindAndRerankPlan.ReadPlan>)
                readLimit -> new FindAndRerankPlan.FilterReadPlan(FIELD_SORTS, readLimit)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("readRankSources")
  void readHasRankSourceOfItsSort(
      String description, FindAndRerankPlan.ReadPlan read, Rank.RankSource rankSource) {
    assertThat(read.rankSource()).as(description).isEqualTo(rankSource);
  }

  static Stream<Arguments> readRankSources() {
    return Stream.of(
        Arguments.of("vector read", VECTOR_READ, Rank.RankSource.VECTOR),
        Arguments.of("vectorize read", VECTORIZE_READ, Rank.RankSource.VECTOR),
        Arguments.of("lexical read", LEXICAL_READ, Rank.RankSource.BM25),
        Arguments.of("filter read", FILTER_READ, Rank.RankSource.FILTER));
  }

  @Test
  void vectorReadsWithTheSameValuesAreEqual() {
    var read =
        new FindAndRerankPlan.VectorReadPlan(
            new VectorSortExpression(new float[] {0.1f, 0.2f}), 50);
    var equalRead =
        new FindAndRerankPlan.VectorReadPlan(
            new VectorSortExpression(new float[] {0.1f, 0.2f}), 50);

    assertThat(read).isEqualTo(equalRead).hasSameHashCodeAs(equalRead);
    assertThat(equalRead).isEqualTo(read);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("differentVectorReads")
  void vectorReadsThatDifferInOneFieldAreNotEqual(
      String description,
      FindAndRerankPlan.VectorReadPlan read,
      FindAndRerankPlan.VectorReadPlan otherRead) {
    assertThat(read).as(description).isNotEqualTo(otherRead);
    assertThat(otherRead).as(description).isNotEqualTo(read);
  }

  static Stream<Arguments> differentVectorReads() {
    return Stream.of(
        Arguments.of(
            "different vector values",
            new FindAndRerankPlan.VectorReadPlan(
                new VectorSortExpression(new float[] {0.1f, 0.2f}), 50),
            new FindAndRerankPlan.VectorReadPlan(
                new VectorSortExpression(new float[] {0.1f, 0.3f}), 50)),
        Arguments.of(
            "different readLimit",
            new FindAndRerankPlan.VectorReadPlan(new VectorSortExpression(VECTOR), 50),
            new FindAndRerankPlan.VectorReadPlan(new VectorSortExpression(VECTOR), 20)));
  }

  @Test
  void filterReadCopiesTheSorts() {
    var sorts = new ArrayList<>(FIELD_SORTS);
    var read = new FindAndRerankPlan.FilterReadPlan(sorts, 50);

    sorts.add(new FieldSortExpression("city", true));

    assertThat(read.sorts()).isEqualTo(FIELD_SORTS);
  }

  @Test
  void filterReadSortsCannotBeChanged() {
    // a list that can be changed, so only the read's own copy makes the sorts unmodifiable
    var read = new FindAndRerankPlan.FilterReadPlan(new ArrayList<>(FIELD_SORTS), 50);

    assertThatThrownBy(() -> read.sorts().add(new FieldSortExpression("city", true)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ===========================================================================================
  // RerankStep
  // ===========================================================================================

  @Test
  void rejectsLimitBelowOne() {
    assertThatThrownBy(() -> rerankStep(0))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("limit must be at least 1, limit=0");
  }

  @Test
  void acceptsLimitOfOne() {
    assertThatCode(() -> rerankStep(1)).doesNotThrowAnyException();
  }

  // ===========================================================================================
  // Helpers / no more tests below
  // ===========================================================================================

  private static FindAndRerankPlan.RerankStep rerankStep(int limit) {
    return new FindAndRerankPlan.RerankStep(
        new CollectionRerankDef.RerankServiceDef("nvidia", TestConstants.RERANK_MODEL, null, null),
        mock(RerankingQuery.class),
        PathMatchLocator.forPath("body"),
        DocumentProjector.defaultProjector(),
        limit);
  }
}
