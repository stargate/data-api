package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FindAndRerankPlan} and its records: the rules their constructors check, how a
 * {@link FindAndRerankPlan.RerankLeg} compares, and that the legs of a plan cannot change.
 */
class FindAndRerankPlanTest {

  private static final float[] VECTOR = {0.1f, 0.2f};

  private static final FindAndRerankPlan.RerankLeg VECTOR_LEG =
      new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, VECTOR, null, 50);

  /** A VECTOR_LEG that reads with the vector from the vectorize step. */
  private static final FindAndRerankPlan.RerankLeg VECTORIZED_LEG =
      new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, 50);

  private static final FindAndRerankPlan.RerankLeg LEXICAL_LEG =
      new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, "cows", 50);

  private static final FindAndRerankPlan.VectorizeStep VECTORIZE =
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
      FindAndRerankPlan.VectorizeStep vectorize,
      List<FindAndRerankPlan.RerankLeg> legs,
      String message) {
    assertThatThrownBy(() -> new FindAndRerankPlan(vectorize, legs, RERANK_STEP, false, false))
        .as(description)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message);
  }

  static Stream<Arguments> invalidPlans() {
    return Stream.of(
        Arguments.of("legs empty", null, List.of(), "legs must not be empty"),
        Arguments.of(
            "two legs of one type",
            null,
            List.of(VECTOR_LEG, LEXICAL_LEG, LEXICAL_LEG),
            "legs must have at most one of each type, legs=[VECTOR_LEG, LEXICAL_LEG, LEXICAL_LEG]"),
        Arguments.of(
            "VECTOR_LEG without a vector, no vectorize step",
            null,
            List.of(VECTORIZED_LEG),
            "vectorize must not be null when the VECTOR_LEG has no vector"),
        Arguments.of(
            "vectorize step, the VECTOR_LEG has a vector",
            VECTORIZE,
            List.of(VECTOR_LEG),
            "vectorize must be null when there is no VECTOR_LEG without a vector"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("validPlans")
  void acceptsValidPlan(
      String description,
      FindAndRerankPlan.VectorizeStep vectorize,
      List<FindAndRerankPlan.RerankLeg> legs) {
    assertThatCode(() -> new FindAndRerankPlan(vectorize, legs, RERANK_STEP, false, false))
        .as(description)
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validPlans() {
    return Stream.of(
        Arguments.of("one LEXICAL_LEG, no VECTOR_LEG needed", null, List.of(LEXICAL_LEG)),
        Arguments.of("VECTOR_LEG with a vector, no vectorize step", null, List.of(VECTOR_LEG)),
        Arguments.of(
            "VECTOR_LEG without a vector, vectorize step, LEXICAL_LEG",
            VECTORIZE,
            List.of(VECTORIZED_LEG, LEXICAL_LEG)));
  }

  @Test
  void copiesTheLegs() {
    var legs = new ArrayList<>(List.of(VECTOR_LEG));
    var plan = new FindAndRerankPlan(null, legs, RERANK_STEP, false, false);

    legs.add(LEXICAL_LEG);

    assertThat(plan.legs()).containsExactly(VECTOR_LEG);
  }

  @Test
  void legsCannotBeChanged() {
    // a list that can be changed, so only the plan's own copy makes the legs unmodifiable
    var plan =
        new FindAndRerankPlan(
            null, new ArrayList<>(List.of(VECTOR_LEG)), RERANK_STEP, false, false);

    assertThatThrownBy(() -> plan.legs().add(LEXICAL_LEG))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // ===========================================================================================
  // RerankLeg
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidLegs")
  void rejectsInvalidLeg(
      String description,
      RerankLegType legType,
      float[] vector,
      String lexicalText,
      Class<? extends Throwable> exceptionClass,
      String message) {
    assertThatThrownBy(() -> new FindAndRerankPlan.RerankLeg(legType, vector, lexicalText, 50))
        .as(description)
        .isExactlyInstanceOf(exceptionClass)
        .hasMessage(message);
  }

  static Stream<Arguments> invalidLegs() {
    return Stream.of(
        Arguments.of(
            "VECTOR_LEG with lexicalText",
            RerankLegType.VECTOR_LEG,
            null,
            "cows",
            IllegalArgumentException.class,
            "lexicalText must be null for a VECTOR_LEG"),
        Arguments.of(
            "LEXICAL_LEG without lexicalText",
            RerankLegType.LEXICAL_LEG,
            null,
            null,
            NullPointerException.class,
            "lexicalText must not be null for a LEXICAL_LEG"),
        Arguments.of(
            "LEXICAL_LEG with a vector",
            RerankLegType.LEXICAL_LEG,
            VECTOR,
            "cows",
            IllegalArgumentException.class,
            "vector must be null for a LEXICAL_LEG"),
        Arguments.of(
            "FILTER_LEG with a vector",
            RerankLegType.FILTER_LEG,
            VECTOR,
            null,
            IllegalArgumentException.class,
            "vector must be null for a FILTER_LEG"),
        Arguments.of(
            "FILTER_LEG with lexicalText",
            RerankLegType.FILTER_LEG,
            null,
            "cows",
            IllegalArgumentException.class,
            "lexicalText must be null for a FILTER_LEG"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("validLegs")
  void acceptsValidLeg(
      String description, RerankLegType legType, float[] vector, String lexicalText) {
    assertThatCode(() -> new FindAndRerankPlan.RerankLeg(legType, vector, lexicalText, 50))
        .as(description)
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validLegs() {
    return Stream.of(
        Arguments.of("VECTOR_LEG with a vector", RerankLegType.VECTOR_LEG, VECTOR, null),
        Arguments.of("VECTOR_LEG without a vector", RerankLegType.VECTOR_LEG, null, null),
        Arguments.of("LEXICAL_LEG with lexicalText", RerankLegType.LEXICAL_LEG, null, "cows"),
        Arguments.of("FILTER_LEG with neither", RerankLegType.FILTER_LEG, null, null));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("equalLegs")
  void legsWithTheSameValuesAreEqual(
      String description, FindAndRerankPlan.RerankLeg leg, FindAndRerankPlan.RerankLeg equalLeg) {
    assertThat(leg).as(description).isEqualTo(equalLeg).hasSameHashCodeAs(equalLeg);
    assertThat(equalLeg).as(description).isEqualTo(leg);
  }

  static Stream<Arguments> equalLegs() {
    return Stream.of(
        Arguments.of(
            "the same vector values in two arrays",
            new FindAndRerankPlan.RerankLeg(
                RerankLegType.VECTOR_LEG, new float[] {0.1f, 0.2f}, null, 50),
            new FindAndRerankPlan.RerankLeg(
                RerankLegType.VECTOR_LEG, new float[] {0.1f, 0.2f}, null, 50)),
        Arguments.of(
            "no vector",
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, 50),
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, 50)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("differentLegs")
  void legsThatDifferInOneFieldAreNotEqual(
      String description, FindAndRerankPlan.RerankLeg leg, FindAndRerankPlan.RerankLeg otherLeg) {
    assertThat(leg).as(description).isNotEqualTo(otherLeg);
    assertThat(otherLeg).as(description).isNotEqualTo(leg);
  }

  static Stream<Arguments> differentLegs() {
    return Stream.of(
        Arguments.of(
            "different legType",
            new FindAndRerankPlan.RerankLeg(RerankLegType.FILTER_LEG, null, null, 50),
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, 50)),
        Arguments.of(
            "different vector values",
            new FindAndRerankPlan.RerankLeg(
                RerankLegType.VECTOR_LEG, new float[] {0.1f, 0.2f}, null, 50),
            new FindAndRerankPlan.RerankLeg(
                RerankLegType.VECTOR_LEG, new float[] {0.1f, 0.3f}, null, 50)),
        Arguments.of(
            "a vector and no vector",
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, VECTOR, null, 50),
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, 50)),
        Arguments.of(
            "different lexicalText",
            new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, "cows", 50),
            new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, "cheese", 50)),
        Arguments.of(
            "different readLimit",
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, VECTOR, null, 50),
            new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, VECTOR, null, 20)));
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
