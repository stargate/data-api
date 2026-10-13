package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import java.util.Objects;
import java.util.Optional;

/**
 * A leg that reads the documents with the best BM25 score for a lexical query, the collection must
 * have a lexical index.
 *
 * @param lexicalQuery The text to search for.
 * @param readLimit The maximum number of documents to read.
 */
record LexicalLeg(String lexicalQuery, int readLimit) implements RerankLeg {

  LexicalLeg {
    Objects.requireNonNull(lexicalQuery, "lexicalQuery must not be null");
  }

  @Override
  public Rank.RankSource rankSource() {
    return Rank.RankSource.BM25;
  }

  @Override
  public Optional<String> defaultRerankQuery() {
    return Optional.empty();
  }

  @Override
  public Optional<String> defaultPassageField() {
    return Optional.empty();
  }

  @Override
  public InnerRead buildInnerRead(
      FilterDefinition filterDefinition, boolean includeScores, boolean includeSortVector) {

    return InnerRead.create(
        filterDefinition,
        SortClause.immutable(SortExpression.collectionLexicalSort(lexicalQuery)),
        readLimit,
        includeScores,
        includeSortVector,
        null);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder.append("lexicalQuery", lexicalQuery).append("readLimit", readLimit);
  }
}
