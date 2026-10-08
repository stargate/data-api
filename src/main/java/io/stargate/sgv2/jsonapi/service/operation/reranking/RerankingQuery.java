package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

/**
 * The reranking query to use for a reranking operation. Either from the $vectorize sort clause or
 * manually specified by the user.
 */
public class RerankingQuery implements Recordable {

  public enum Source {
    /** Query came from the $vectorize sort in the user query. This is the default. */
    VECTORIZE,
    /** Query came from the options.rerankQuery property in the user query. */
    OPTIONS,
  }

  private final String query;
  private final Source source;

  private RerankingQuery(String query, Source source) {
    this.query = Objects.requireNonNull(query, "query must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
  }

  public String query() {
    return query;
  }

  public Source source() {
    return source;
  }

  /**
   * Creates a new RerankingQuery from the values in the users command. The query from the options
   * is used before the <code>$vectorize</code> text.
   *
   * <p>Throws {@link RequestException.Code#MISSING_RERANK_QUERY_TEXT} if both values are null or
   * blank.
   *
   * @param optionsQuery The <code>options.rerankQuery</code> from the command, may be null.
   * @param vectorizeQuery The text of the <code>$vectorize</code> sort, may be null.
   * @return Constructed RerankingQuery, with the source indicating where the query came from.
   */
  public static RerankingQuery create(String optionsQuery, String vectorizeQuery) {

    if (optionsQuery != null && !optionsQuery.isBlank()) {
      return new RerankingQuery(optionsQuery, Source.OPTIONS);
    }
    if (vectorizeQuery != null && !vectorizeQuery.isBlank()) {
      return new RerankingQuery(vectorizeQuery, Source.VECTORIZE);
    }
    throw RequestException.Code.MISSING_RERANK_QUERY_TEXT.get();
  }

  @Override
  public String toString() {
    return PrettyPrintable.print(this);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder.append("query", query).append("source", source);
  }
}
