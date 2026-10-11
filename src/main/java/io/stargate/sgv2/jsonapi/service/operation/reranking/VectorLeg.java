package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * A leg that reads the documents most similar to a vector the user provided.
 *
 * <p>NOTE: the vector array is not copied, it is the vector from the parsed sort and nothing
 * changes it after parsing. Do not change the array returned by {@link #vector()}.
 *
 * @param vector The vector to sort by.
 * @param readLimit The maximum number of documents to read.
 */
record VectorLeg(float[] vector, int readLimit) implements RerankLeg {

  VectorLeg {
    Objects.requireNonNull(vector, "vector must not be null");
  }

  @Override
  public Rank.RankSource rankSource() {
    return Rank.RankSource.VECTOR;
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
        SortClause.immutable(SortExpression.collectionVectorSort(vector)),
        readLimit,
        includeScores,
        includeSortVector,
        null);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    // the length only, vectors can be long
    return dataRecorder.append("vector.length", vector.length).append("readLimit", readLimit);
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

  @Override
  public String toString() {
    return PrettyPrintable.print(this);
  }
}
