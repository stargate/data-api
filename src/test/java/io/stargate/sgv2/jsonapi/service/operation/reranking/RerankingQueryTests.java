package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.exception.RequestException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link RerankingQuery}. */
public class RerankingQueryTests {

  private static final String VECTORIZE_QUERY = "vetorize-query-" + System.currentTimeMillis();
  private static final String OPTIONS_QUERY = "options-query-" + System.currentTimeMillis();

  /**
   * Rows of options.rerankQuery, the $vectorize text, then the expected query and source, or a null
   * query and source when MISSING_RERANK_QUERY_TEXT is expected.
   */
  private static Stream<Arguments> createCases() {
    return Stream.of(
        // the $vectorize text when there is no usable options.rerankQuery
        Arguments.of(null, VECTORIZE_QUERY, VECTORIZE_QUERY, RerankingQuery.Source.VECTORIZE),
        Arguments.of("", VECTORIZE_QUERY, VECTORIZE_QUERY, RerankingQuery.Source.VECTORIZE),
        Arguments.of("   ", VECTORIZE_QUERY, VECTORIZE_QUERY, RerankingQuery.Source.VECTORIZE),
        // options.rerankQuery wins over the $vectorize text
        Arguments.of(OPTIONS_QUERY, VECTORIZE_QUERY, OPTIONS_QUERY, RerankingQuery.Source.OPTIONS),
        Arguments.of(OPTIONS_QUERY, null, OPTIONS_QUERY, RerankingQuery.Source.OPTIONS),
        // MISSING_RERANK_QUERY_TEXT when neither is usable
        Arguments.of(null, "", null, null),
        Arguments.of(null, "   ", null, null),
        Arguments.of("", null, null, null),
        Arguments.of("   ", null, null, null),
        Arguments.of(null, null, null, null),
        Arguments.of("", "", null, null),
        Arguments.of("   ", "   ", null, null));
  }

  @ParameterizedTest(name = "optionsQuery=''{0}'', vectorizeQuery=''{1}''")
  @MethodSource("createCases")
  public void create(
      String optionsQuery,
      String vectorizeQuery,
      String expectedQuery,
      RerankingQuery.Source expectedSource) {

    if (expectedQuery == null) {
      var ex =
          assertThrowsExactly(
              RequestException.class, () -> RerankingQuery.create(optionsQuery, vectorizeQuery));
      assertThat(ex.code)
          .as("error code is " + RequestException.Code.MISSING_RERANK_QUERY_TEXT.name())
          .isEqualTo(RequestException.Code.MISSING_RERANK_QUERY_TEXT.name());
      return;
    }

    var query = RerankingQuery.create(optionsQuery, vectorizeQuery);
    assertThat(query.query()).as("rerank query").isEqualTo(expectedQuery);
    assertThat(query.source()).as("rerank query source").isEqualTo(expectedSource);
  }

  @Test
  public void testToString() {
    var query = RerankingQuery.create(null, VECTORIZE_QUERY);
    assertThat(query.toString())
        .as("toString is correct")
        .isEqualTo("RerankingQuery{query=" + VECTORIZE_QUERY + ", source=VECTORIZE}");
  }
}
