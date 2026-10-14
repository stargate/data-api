package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for {@link FindAndRerankPlan}. */
public class FindAndRerankPlanTests {

  private static final CollectionRerankDef.RerankServiceDef RERANK_SERVICE =
      new CollectionRerankDef.RerankServiceDef("nvidia", TestConstants.RERANK_MODEL, null, null);
  private static final LexicalLeg LEXICAL_LEG = new LexicalLeg("cows", 20);
  private static final VectorLeg VECTOR_LEG = new VectorLeg(new float[] {0.1f, 0.2f, 0.3f}, 40);

  @Test
  void failEmptyLegs() {
    var rerankingQuery = RerankingQuery.create("q", null);
    var passageLocator = PathMatchLocator.forPath("body");

    var ex =
        assertThrowsExactly(
            IllegalArgumentException.class,
            () ->
                new FindAndRerankPlan(
                    List.of(), RERANK_SERVICE, rerankingQuery, passageLocator, 10));
    assertThat(ex.getMessage()).isEqualTo("legs must not be empty");
  }

  @Test
  void legsAreCopied() {
    var legs = new ArrayList<RerankLeg>(List.of(LEXICAL_LEG));
    var plan =
        new FindAndRerankPlan(
            legs,
            RERANK_SERVICE,
            RerankingQuery.create("q", null),
            PathMatchLocator.forPath("body"),
            10);
    legs.add(VECTOR_LEG);

    assertThat(plan.legs()).containsExactly(LEXICAL_LEG);
  }

  @Test
  void recordsLegsWithoutServiceAuthentication() {
    var plan =
        new FindAndRerankPlan(
            List.of(LEXICAL_LEG, VECTOR_LEG),
            new CollectionRerankDef.RerankServiceDef(
                "nvidia", TestConstants.RERANK_MODEL, Map.of("providerKey", "secret"), null),
            RerankingQuery.create("q", null),
            PathMatchLocator.forPath("body"),
            10);

    assertThat(plan.toString())
        .contains(
            "lexicalQuery=cows",
            "vector.length=3",
            "rerankServiceDef.modelName=" + TestConstants.RERANK_MODEL,
            "passageLocator.path=body",
            "limit=10")
        .doesNotContain("secret");
  }
}
