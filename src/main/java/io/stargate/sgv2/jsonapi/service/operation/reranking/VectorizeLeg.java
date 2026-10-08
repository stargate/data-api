package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;

/**
 * A leg that embeds the text into a vector with the collection's vectorize service, then reads the
 * documents most similar to that vector.
 *
 * <p>The text is also the default rerank query, and the <code>$vectorize</code> field is the
 * default passage to rerank on.
 *
 * @param vectorizeText The text to embed.
 * @param dimension The dimension of the collection's vector.
 * @param vectorizeDefinition The vectorize service of the collection.
 * @param readLimit The maximum number of documents to read.
 */
public record VectorizeLeg(
    String vectorizeText, int dimension, VectorizeDefinition vectorizeDefinition, int readLimit)
    implements RerankLeg {

  public VectorizeLeg {
    Objects.requireNonNull(vectorizeText, "vectorizeText must not be null");
    Objects.requireNonNull(vectorizeDefinition, "vectorizeDefinition must not be null");
  }

  @Override
  public Rank.RankSource rankSource() {
    return Rank.RankSource.VECTOR;
  }

  @Override
  public boolean needsVectorize() {
    return true;
  }

  @Override
  public Optional<String> defaultRerankQuery() {
    return Optional.of(vectorizeText);
  }

  @Override
  public Optional<String> defaultPassageField() {
    return Optional.of(VECTOR_EMBEDDING_TEXT_FIELD);
  }

  @Override
  public InnerRead buildInnerRead(
      FilterDefinition filterDefinition, boolean includeScores, boolean includeSortVector) {

    // must be a new and mutable clause for every read, when the vector is ready the deferred
    // vectorize replaces what is in the clause with the vector sort, and so does the read task
    var sortClause = new SortClause(new ArrayList<>());
    var deferredVectorize =
        new DeferredVectorize(vectorizeText, dimension, vectorizeDefinition, sortClause);
    return InnerRead.create(
        filterDefinition,
        sortClause,
        readLimit,
        includeScores,
        includeSortVector,
        deferredVectorize);
  }
}
