package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import java.util.ArrayList;
import java.util.Objects;

/**
 * A leg that embeds the text into a vector with the collection's vectorize service, then reads the
 * documents most similar to that vector.
 *
 * @param vectorizeText The text to embed.
 * @param dimension The dimension of the collection's vector.
 * @param vectorizeDefinition The vectorize service of the collection.
 * @param readLimit The maximum number of documents to read.
 */
record VectorizeLeg(
    String vectorizeText, int dimension, VectorizeDefinition vectorizeDefinition, int readLimit)
    implements RerankLeg {

  VectorizeLeg {
    Objects.requireNonNull(vectorizeText, "vectorizeText must not be null");
    Objects.requireNonNull(vectorizeDefinition, "vectorizeDefinition must not be null");
  }

  @Override
  public Rank.RankSource rankSource() {
    return Rank.RankSource.VECTOR;
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

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("vectorizeText", vectorizeText)
        .append("dimension", dimension)
        .append("vectorizeDefinition.provider", vectorizeDefinition.provider())
        .append("vectorizeDefinition.modelName", vectorizeDefinition.modelName())
        .append("readLimit", readLimit);
  }
}
