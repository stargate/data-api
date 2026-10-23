package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.exception.RequestException;
import org.junit.jupiter.api.Test;

/** Tests for {@link RerankingQuery}. */
public class RerankingQueryTests {

  private static final String VECTORIZE_QUERY = "vetorize-query-" + System.currentTimeMillis();
  private static final String OPTIONS_QUERY = "options-query-" + System.currentTimeMillis();

  @Test
  public void createFromVectorize() {

    var query = RerankingQuery.create(null, VECTORIZE_QUERY);
    assertThat(query.query()).as("vectorize query used as rerank query").isEqualTo(VECTORIZE_QUERY);
    assertThat(query.source()).as("source is vectorize").isEqualTo(RerankingQuery.Source.VECTORIZE);

    for (var blankOptions : new String[] {"", "   "}) {
      var blankOptionsQuery = RerankingQuery.create(blankOptions, VECTORIZE_QUERY);
      assertThat(blankOptionsQuery.query())
          .as("vectorize query used when options.rerankQuery is '%s'", blankOptions)
          .isEqualTo(VECTORIZE_QUERY);
      assertThat(blankOptionsQuery.source())
          .as("source is vectorize when options.rerankQuery is '%s'", blankOptions)
          .isEqualTo(RerankingQuery.Source.VECTORIZE);
    }

    assertMissingQueryText("error when blank vectorize", null, "");
    assertMissingQueryText("error when whitespace vectorize", null, "   ");
  }

  @Test
  public void createFromOptions() {

    var query = RerankingQuery.create(OPTIONS_QUERY, VECTORIZE_QUERY);
    assertThat(query.query())
        .as("use the options.rerankQuery when both vectorize and options.rerankQuery are present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);

    var query2 = RerankingQuery.create(OPTIONS_QUERY, null);
    assertThat(query2.query())
        .as("use the options.rerankQuery when no vectorize query is present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query2.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);

    assertMissingQueryText("error when blank options.rerankQuery", "", null);
    assertMissingQueryText("error when whitespace options.rerankQuery", "   ", null);
  }

  @Test
  public void createMissingQuery() {

    assertMissingQueryText("error when no sort or options", null, null);
    assertMissingQueryText("error when both values are blank", "", "");
    assertMissingQueryText("error when both values are whitespace", "   ", "   ");
  }

  private void assertMissingQueryText(String context, String optionsQuery, String vectorizeQuery) {
    var ex =
        assertThrowsExactly(
            RequestException.class,
            () -> RerankingQuery.create(optionsQuery, vectorizeQuery),
            context);

    assertThat(ex.code)
        .as("error code is " + RequestException.Code.MISSING_RERANK_QUERY_TEXT.name())
        .isEqualTo(RequestException.Code.MISSING_RERANK_QUERY_TEXT.name());
  }

  @Test
  public void testToString() {
    var query = RerankingQuery.create(null, VECTORIZE_QUERY);
    assertThat(query.toString())
        .as("toString is correct")
        .isEqualTo("RerankingQuery{query=" + VECTORIZE_QUERY + ", source=VECTORIZE}");
  }
}
