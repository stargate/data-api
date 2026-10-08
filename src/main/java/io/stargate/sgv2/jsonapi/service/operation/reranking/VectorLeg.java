package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * A leg that reads the documents most similar to a vector the user provided.
 *
 * @param vector The vector to sort by.
 * @param readLimit The maximum number of documents to read.
 */
public record VectorLeg(float[] vector, int readLimit) implements RerankLeg {

  public VectorLeg {
    Objects.requireNonNull(vector, "vector must not be null");
  }

  @Override
  public Rank.RankSource rankSource() {
    return Rank.RankSource.VECTOR;
  }

  @Override
  public boolean needsVectorize() {
    return false;
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

    var sortClause = new SortClause(new ArrayList<>());
    sortClause.sortExpressions().add(SortExpression.collectionVectorSort(vector));
    return InnerRead.create(
        filterDefinition, sortClause, readLimit, includeScores, includeSortVector, null);
  }

  /** Override to do a value equality check on the vector */
  @Override
  public boolean equals(Object obj) {
    return (obj instanceof VectorLeg other)
        && Arrays.equals(vector, other.vector)
        && readLimit == other.readLimit;
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(vector), readLimit);
  }

  /** Override to print the vector values, not the array reference */
  @Override
  public String toString() {
    return "VectorLeg[vector=" + Arrays.toString(vector) + ", readLimit=" + readLimit + "]";
  }
}
