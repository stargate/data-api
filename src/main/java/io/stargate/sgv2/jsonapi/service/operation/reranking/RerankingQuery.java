package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
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
   * Creates a new RerankingQuery from the users command.
   *
   * <p>Throws {@link RequestException.Code#MISSING_RERANK_QUERY_TEXT} if it cannot be determined.
   *
   * @param command Command the user sent.
   * @return Constructed RerankingQuery, with the source indicating where the query came from.
   */
  public static RerankingQuery create(FindAndRerankCommand command) {

    Objects.requireNonNull(command, "command must not be null");

    var rerankQuery =
        getOrDefault(command.options(), FindAndRerankCommand.Options::rerankQuery, "");
    if (rerankQuery != null && !rerankQuery.isBlank()) {
      return new RerankingQuery(rerankQuery, Source.OPTIONS);
    }

    var vectorizeQuery = command.sortClause().vectorizeSort();
    // will never be blank, but double checking for safety
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
