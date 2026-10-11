package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link RerankLeg} implementations {@link LexicalLeg}, {@link VectorLeg} and {@link
 * VectorizeLeg}, and the {@link RerankLeg.InnerRead} they build. Plain JUnit without Quarkus.
 */
public class RerankLegTests {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final VectorizeDefinition VECTORIZE_DEF =
      new VectorizeDefinition("custom", "custom", null, null);
  private static final int DIMENSION = 3;
  private static final float[] VECTOR = {0.1f, 0.2f, 0.3f};

  private final FilterDefinition filter = filter("{'name': 'cheese'}");

  /** Every combination of the includeScores and includeSortVector options. */
  private static Stream<Arguments> includeOptions() {
    return Stream.of(
        Arguments.of(false, false),
        Arguments.of(false, true),
        Arguments.of(true, false),
        Arguments.of(true, true));
  }

  @Test
  void lexicalLegDefaults() {
    var leg = new LexicalLeg("cows", 20);

    assertThat(leg.rankSource()).isEqualTo(Rank.RankSource.BM25);
    assertThat(leg.defaultRerankQuery()).isEmpty();
    assertThat(leg.defaultPassageField()).isEmpty();
  }

  @ParameterizedTest(name = "includeScores={0}, includeSortVector={1}")
  @MethodSource("includeOptions")
  void lexicalLegReadNeverIncludesSimilarity(boolean includeScores, boolean includeSortVector) {
    var read = new LexicalLeg("cows", 20).buildInnerRead(filter, includeScores, includeSortVector);

    assertThat(read.deferredVectorize()).isNull();
    assertThat(read.deferrables()).isEmpty();
    assertFindCommand(read.findCommand(), 20, false, includeSortVector);
    assertThat(sortClause(read.findCommand()).sortExpressions())
        .containsExactly(SortExpression.collectionLexicalSort("cows"));
  }

  @Test
  void vectorLegDefaults() {
    var leg = new VectorLeg(VECTOR, 40);

    assertThat(leg.rankSource()).isEqualTo(Rank.RankSource.VECTOR);
    assertThat(leg.defaultRerankQuery()).isEmpty();
    assertThat(leg.defaultPassageField()).isEmpty();
  }

  @ParameterizedTest(name = "includeScores={0}, includeSortVector={1}")
  @MethodSource("includeOptions")
  void vectorLegReadIncludesSimilarityForIncludeScores(
      boolean includeScores, boolean includeSortVector) {
    var read = new VectorLeg(VECTOR, 40).buildInnerRead(filter, includeScores, includeSortVector);

    assertThat(read.deferredVectorize()).isNull();
    assertThat(read.deferrables()).isEmpty();
    assertFindCommand(read.findCommand(), 40, includeScores, includeSortVector);
    assertThat(sortClause(read.findCommand()).sortExpressions())
        .containsExactly(SortExpression.collectionVectorSort(VECTOR));
  }

  @Test
  void vectorLegRecordsVectorLengthNotValues() {
    assertThat(new VectorLeg(VECTOR, 40).toString())
        .isEqualTo("VectorLeg{vector.length=3, readLimit=40}");
  }

  @Test
  void vectorizeLegDefaults() {
    var leg = new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, 30);

    assertThat(leg.rankSource()).isEqualTo(Rank.RankSource.VECTOR);
    assertThat(leg.defaultRerankQuery()).contains("cheese");
    assertThat(leg.defaultPassageField())
        .contains(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
  }

  @ParameterizedTest(name = "includeScores={0}, includeSortVector={1}")
  @MethodSource("includeOptions")
  void vectorizeLegReadIncludesSimilarityForIncludeScores(
      boolean includeScores, boolean includeSortVector) {
    var read =
        new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, 30)
            .buildInnerRead(filter, includeScores, includeSortVector);

    assertFindCommand(read.findCommand(), 30, includeScores, includeSortVector);
  }

  @Test
  void vectorizeLegReadSortsByEmbeddingWhenReady() {
    var read =
        new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, 30).buildInnerRead(filter, true, true);

    assertThat(read.deferrables())
        .as("the embedding task completes the deferred vectorize")
        .containsExactly(read.deferredVectorize());
    var action = (EmbeddingDeferredAction) read.deferredVectorize().deferredAction();
    assertThat(action.startEmbedding()).as("text to embed").isEqualTo("cheese");
    assertThat(action.groupKey().vectorizeDefinition())
        .as("vectorize service")
        .isEqualTo(VECTORIZE_DEF);
    assertThat(action.groupKey().dimension()).as("dimension").isEqualTo(DIMENSION);

    var sortClause = sortClause(read.findCommand());
    assertThat(sortClause.sortExpressions()).as("no sort until the vector is ready").isEmpty();

    // the deferred vectorize updates the sort clause the find command was built with
    var embedding = new float[] {0.4f, 0.5f, 0.6f};
    action.onSuccess(embedding);
    assertThat(sortClause.sortExpressions())
        .containsExactly(SortExpression.collectionVectorSort(embedding));
  }

  @Test
  void vectorizeLegBuildsNewReadEachTime() {
    var leg = new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, 30);

    var first = leg.buildInnerRead(filter, false, false);
    var second = leg.buildInnerRead(filter, false, false);

    assertThat(second.deferredVectorize()).isNotSameAs(first.deferredVectorize());
    assertThat(sortClause(second.findCommand())).isNotSameAs(sortClause(first.findCommand()));
  }

  @Test
  void failInnerReadWithoutSortClause() {
    var ex =
        assertThrowsExactly(
            NullPointerException.class,
            () -> RerankLeg.InnerRead.create(filter, null, 10, false, false, null));
    assertThat(ex).hasMessage("sortClause must not be null");
  }

  private void assertFindCommand(
      FindCommand findCommand,
      int expectedLimit,
      boolean expectedIncludeSimilarity,
      boolean expectedIncludeSortVector) {

    assertThat(findCommand.filterDefinition()).as("filter").isSameAs(filter);
    assertThat(findCommand.projectionDefinition())
        .as("include all projection")
        .isEqualTo(OBJECT_MAPPER.createObjectNode().put("*", 1));
    assertThat(findCommand.options())
        .as("find options")
        .isEqualTo(
            new FindCommand.Options(
                expectedLimit, 0, null, expectedIncludeSimilarity, expectedIncludeSortVector));
  }

  /** The wrapped clause is returned as is, the context is only used to parse a JSON sort. */
  private static SortClause sortClause(FindCommand findCommand) {
    return findCommand.sortDefinition().build(TEST_CONSTANTS.collectionContext());
  }

  /** Parses the filter, the JSON uses single quotes to stay readable. */
  private static FilterDefinition filter(String json) {
    try {
      return new FilterDefinition(
          OBJECT_MAPPER.reader().with(JsonReadFeature.ALLOW_SINGLE_QUOTES).readTree(json));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("invalid filter JSON: " + json, e);
    }
  }
}
