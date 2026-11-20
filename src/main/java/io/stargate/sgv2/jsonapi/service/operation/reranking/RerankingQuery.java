package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

/**
 * The reranking query to use for a reranking operation. Either from the $vectorize or $lexical sort
 * clause, or manually specified by the user.
 */
public class RerankingQuery implements Recordable {

  public enum Source {
    /** Query came from the $vectorize sort in the user query. This is the default. */
    VECTORIZE,
    /** Query came from the $lexical sort in the user query, when the sort is only $lexical. */
    LEXICAL,
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
  public static RerankingQuery forVectorizeSort(String rerankQuery, String vectorizeText) {
    return create(rerankQuery, vectorizeText, Source.VECTORIZE);
  }

  /**
   * Creates a new RerankingQuery for a $lexical sort: the <code>rerankQuery</code> option when it
   * is not blank, otherwise the text of the $lexical sort.
   *
   * <p>Throws {@link RequestException.Code#MISSING_RERANK_QUERY_TEXT} if both are null or blank.
   *
   * @param rerankQuery The <code>rerankQuery</code> option, may be <code>null</code>.
   * @param lexicalText The text of the $lexical sort.
   * @return Constructed RerankingQuery, with the source indicating where the query came from.
   */
  public static RerankingQuery forLexicalSort(String rerankQuery, String lexicalText) {
    return create(rerankQuery, lexicalText, Source.LEXICAL);
  }

  /** The rerankQuery option when it is not blank, otherwise the sort text and its source. */
  private static RerankingQuery create(String rerankQuery, String sortText, Source sortSource) {

    if (rerankQuery != null && !rerankQuery.isBlank()) {
      return new RerankingQuery(rerankQuery, Source.OPTIONS);
    }

    // will never be blank, but double-checking for safety
    if (sortText != null && !sortText.isBlank()) {
      return new RerankingQuery(sortText, sortSource);
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
