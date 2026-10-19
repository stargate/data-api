package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the checks in {@link FindAndRerankPlan} and its steps. */
class FindAndRerankPlanTest {

  private static final FindAndRerankPlan.RerankLeg VECTOR_LEG =
      new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, new float[] {0.1f, 0.2f}, null, 50);

  private static final FindAndRerankPlan.RerankLeg VECTORIZED_LEG =
      new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, 50);

  private static final FindAndRerankPlan.RerankLeg LEXICAL_LEG =
      new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, "cows", 50);

  private static final FindAndRerankPlan.VectorizeStep VECTORIZE =
      new FindAndRerankPlan.VectorizeStep(
          "cheese", 3, new VectorizeDefinition("custom", "custom", null, null));

  private static final FindAndRerankPlan.RerankStep RERANK_STEP = rerankStep(10);

  @Test
  void rejectsEmptyLegs() {
    assertThatThrownBy(() -> new FindAndRerankPlan(null, List.of(), RERANK_STEP, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("legs must not be empty");
  }

  @Test
  void rejectsTwoLegsOfOneType() {
    assertThatThrownBy(
            () ->
                new FindAndRerankPlan(
                    null, List.of(VECTOR_LEG, LEXICAL_LEG, LEXICAL_LEG), RERANK_STEP, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("legs must have at most one of each type");
  }

  @Test
  void vectorizeOnlyWithVectorLegWithoutVector() {
    assertThatThrownBy(
            () -> new FindAndRerankPlan(null, List.of(VECTORIZED_LEG), RERANK_STEP, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("vectorize must not be null when the VECTOR_LEG has no vector");
    assertThatThrownBy(
            () -> new FindAndRerankPlan(VECTORIZE, List.of(VECTOR_LEG), RERANK_STEP, false, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "vectorize must be null when there is no VECTOR_LEG without a vector");

    var plan =
        new FindAndRerankPlan(
            VECTORIZE, List.of(VECTORIZED_LEG, LEXICAL_LEG), RERANK_STEP, false, false);
    assertThat(plan.legs()).containsExactly(VECTORIZED_LEG, LEXICAL_LEG);
  }

  @Test
  void rejectsLimitBelowOne() {
    assertThatThrownBy(() -> rerankStep(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit must be at least 1, limit=0");
  }

  @Test
  void rejectsValuesForTheOtherLegType() {
    assertThatThrownBy(
            () -> new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, "cows", 50))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lexicalText must be null for a VECTOR_LEG");
    assertThatThrownBy(
            () ->
                new FindAndRerankPlan.RerankLeg(
                    RerankLegType.LEXICAL_LEG, new float[] {0.1f}, "cows", 50))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("vector must be null for a LEXICAL_LEG");
    assertThatThrownBy(
            () -> new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, null, 50))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("lexicalText must not be null for a LEXICAL_LEG");
    assertThatThrownBy(
            () ->
                new FindAndRerankPlan.RerankLeg(
                    RerankLegType.FILTER_LEG, new float[] {0.1f}, null, 50))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("vector must be null for a FILTER_LEG");
    assertThatThrownBy(
            () -> new FindAndRerankPlan.RerankLeg(RerankLegType.FILTER_LEG, null, "cows", 50))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lexicalText must be null for a FILTER_LEG");
  }

  @Test
  void comparesTheVectorByValue() {
    var sameVector = new float[] {0.1f, 0.2f};
    var otherVector = new float[] {0.1f, 0.3f};

    assertThat(new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, sameVector, null, 50))
        .isEqualTo(VECTOR_LEG)
        .hasSameHashCodeAs(VECTOR_LEG);
    assertThat(new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, otherVector, null, 50))
        .isNotEqualTo(VECTOR_LEG);
  }

  private static FindAndRerankPlan.RerankStep rerankStep(int limit) {
    return new FindAndRerankPlan.RerankStep(
        new CollectionRerankDef.RerankServiceDef("nvidia", TestConstants.RERANK_MODEL, null, null),
        mock(RerankingQuery.class),
        PathMatchLocator.forPath("body"),
        DocumentProjector.defaultProjector(),
        limit);
  }
}
