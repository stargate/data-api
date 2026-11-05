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
   * Creates a new RerankingQuery: the <code>rerankQuery</code> option when it is not blank,
   * otherwise the text the command vectorizes.
   *
   * <p>Throws {@link RequestException.Code#MISSING_RERANK_QUERY_TEXT} if both are null or blank.
   *
   * @param rerankQuery The <code>rerankQuery</code> option, may be <code>null</code>.
   * @param vectorizeText The text the command vectorizes, <code>null</code> when it does not
   *     vectorize.
   * @return Constructed RerankingQuery, with the source indicating where the query came from.
   */
  public static RerankingQuery create(String rerankQuery, String vectorizeText) {

    if (rerankQuery != null && !rerankQuery.isBlank()) {
      return new RerankingQuery(rerankQuery, Source.OPTIONS);
    }

    // will never be blank, but double-checking for safety
    if (vectorizeText != null && !vectorizeText.isBlank()) {
      return new RerankingQuery(vectorizeText, Source.VECTORIZE);
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
