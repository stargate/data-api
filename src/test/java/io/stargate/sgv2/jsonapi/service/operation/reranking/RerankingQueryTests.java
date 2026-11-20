package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import io.stargate.sgv2.jsonapi.exception.RequestException;
import org.junit.jupiter.api.Test;

/** Tests for {@link RerankingQuery}. */
public class RerankingQueryTests {

  private static final String VECTORIZE_QUERY = "vetorize-query-" + System.currentTimeMillis();
  private static final String OPTIONS_QUERY = "options-query-" + System.currentTimeMillis();
  private static final String LEXICAL_QUERY = "lexical-query-" + System.currentTimeMillis();

  @Test
  public void createFromVectorize() {

    var query = RerankingQuery.forVectorizeSort(null, VECTORIZE_QUERY);
    assertThat(query.query()).as("vectorize query used as rerank query").isEqualTo(VECTORIZE_QUERY);
    assertThat(query.source()).as("source is vectorize").isEqualTo(RerankingQuery.Source.VECTORIZE);

    assertMissingQueryText("error when blank vectorize", null, "");
  }

  @Test
  public void createFromOptions() {

    var query = RerankingQuery.forVectorizeSort(OPTIONS_QUERY, VECTORIZE_QUERY);
    assertThat(query.query())
        .as("use the options.rerankQuery when both vectorize and options.rerankQuery are present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);

    var query2 = RerankingQuery.forVectorizeSort(OPTIONS_QUERY, null);
    assertThat(query2.query())
        .as("use the options.rerankQuery when no vectorize query is present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query2.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);

    assertMissingQueryText("error when blank options.rerankQuery", "", null);
  }

  @Test
  public void createFromLexical() {

    var query = RerankingQuery.forLexicalSort(null, LEXICAL_QUERY);
    assertThat(query.query()).as("lexical text used as rerank query").isEqualTo(LEXICAL_QUERY);
    assertThat(query.source()).as("source is lexical").isEqualTo(RerankingQuery.Source.LEXICAL);

    var query2 = RerankingQuery.forLexicalSort("  ", LEXICAL_QUERY);
    assertThat(query2.query())
        .as("lexical text used as rerank query when options.rerankQuery is blank")
        .isEqualTo(LEXICAL_QUERY);
    assertThat(query2.source()).as("source is lexical").isEqualTo(RerankingQuery.Source.LEXICAL);

    var query3 = RerankingQuery.forLexicalSort(OPTIONS_QUERY, LEXICAL_QUERY);
    assertThat(query3.query())
        .as(
            "use the options.rerankQuery when both lexical text and options.rerankQuery are present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query3.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);
  }

  @Test
  public void createMissingQuery() {

    assertMissingQueryText("error when no sort or options", null, null);
  }

  private void assertMissingQueryText(String context, String rerankQuery, String vectorizeText) {
    var ex =
        assertThrowsExactly(
            RequestException.class,
            () -> RerankingQuery.forVectorizeSort(rerankQuery, vectorizeText),
            context);

    assertThat(ex.code)
        .as("error code is " + RequestException.Code.MISSING_RERANK_QUERY_TEXT.name())
        .isEqualTo(RequestException.Code.MISSING_RERANK_QUERY_TEXT.name());
  }

  @Test
  public void testToString() {
    var query = RerankingQuery.forVectorizeSort(null, VECTORIZE_QUERY);
    assertThat(query.toString())
        .as("toString is correct")
        .isEqualTo("RerankingQuery{query=" + VECTORIZE_QUERY + ", source=VECTORIZE}");
  }
}
