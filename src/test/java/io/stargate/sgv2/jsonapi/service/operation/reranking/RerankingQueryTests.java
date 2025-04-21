package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import org.junit.jupiter.api.Test;

/** Tests for {@link RerankingQuery}. */
public class RerankingQueryTests {

  private static final String VECTORIZE_QUERY = "vetorize-query-" + System.currentTimeMillis();
  private static final String OPTIONS_QUERY = "options-query-" + System.currentTimeMillis();

  private static final FindAndRerankSort vectorizeSort;
  private static final FindAndRerankSort blankVectorizeSort;
  private static final FindAndRerankSort noSort;

  private static final FindAndRerankCommand.Options rerankQueryOptions;
  private static final FindAndRerankCommand.Options blankRerankQueryOptions;

  static {
    vectorizeSort = mock(FindAndRerankSort.class);
    when(vectorizeSort.vectorizeSort()).thenReturn(VECTORIZE_QUERY);

    blankVectorizeSort = mock(FindAndRerankSort.class);
    when(blankVectorizeSort.vectorizeSort()).thenReturn("");

    noSort = mock(FindAndRerankSort.class);
    when(noSort.vectorizeSort()).thenReturn(null);

    rerankQueryOptions = mock(FindAndRerankCommand.Options.class);
    when(rerankQueryOptions.rerankQuery()).thenReturn(OPTIONS_QUERY);

    blankRerankQueryOptions = mock(FindAndRerankCommand.Options.class);
    when(blankRerankQueryOptions.rerankQuery()).thenReturn("");
  }

  private static FindAndRerankCommand command(
      FindAndRerankSort sort, FindAndRerankCommand.Options options) {
    return new FindAndRerankCommand(mock(FilterSpec.class), mock(JsonNode.class), sort, options);
  }

  @Test
  public void createFromVectorize() {

    var command = command(vectorizeSort, null);

    var query = RerankingQuery.create(command);
    assertThat(query.query()).as("vectorize query used as rerank query").isEqualTo(VECTORIZE_QUERY);
    assertThat(query.source()).as("source is vectorize").isEqualTo(RerankingQuery.Source.VECTORIZE);

    var command2 = command(blankVectorizeSort, null);
    assertMissingQueryText("error when blank vectorize", command2);
  }

  @Test
  public void createFromOptions() {

    var commandVectorizeAndOptions = command(vectorizeSort, rerankQueryOptions);
    var query = RerankingQuery.create(commandVectorizeAndOptions);
    assertThat(query.query())
        .as("use the options.rerankQuery when both vectorize and options.rerankQuery are present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);

    var commandOptionsOnly = command(noSort, rerankQueryOptions);
    var query2 = RerankingQuery.create(commandOptionsOnly);
    assertThat(query2.query())
        .as("use the options.rerankQuery when no vectorize query is present")
        .isEqualTo(OPTIONS_QUERY);
    assertThat(query2.source()).as("source is options").isEqualTo(RerankingQuery.Source.OPTIONS);

    var commandBlankOptions = command(noSort, blankRerankQueryOptions);
    assertMissingQueryText("error when blank options.rerankQuery", commandBlankOptions);
  }

  @Test
  public void createMissingQuery() {

    var command = command(noSort, mock(FindAndRerankCommand.Options.class));
    assertMissingQueryText("error when no sort or options", command);
  }

  private void assertMissingQueryText(String context, FindAndRerankCommand command) {
    var ex =
        assertThrowsExactly(RequestException.class, () -> RerankingQuery.create(command), context);

    assertThat(ex.code)
        .as("error code is " + RequestException.Code.MISSING_RERANK_QUERY_TEXT.name())
        .isEqualTo(RequestException.Code.MISSING_RERANK_QUERY_TEXT.name());
  }

  @Test
  public void testToString() {
    var command = command(vectorizeSort, null);
    var query = RerankingQuery.create(command);
    assertThat(query.toString())
        .as("toString is correct")
        .isEqualTo("RerankingQuery{query=" + VECTORIZE_QUERY + ", source=VECTORIZE}");
  }
}
