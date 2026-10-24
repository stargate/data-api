package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The steps a findAndRerank command runs: vectorize the sort text when there is a {@link
 * VectorizeStep}, read with each {@link RerankLeg}, then rerank what the legs read with the {@link
 * RerankStep}.
 *
 * <p>Made by {@link FindAndRerankPlanner}, {@link FindAndRerankOperationBuilder} builds the tasks
 * from it.
 *
 * @param vectorize The vectorize step for the vector leg, <code>null</code> when the command does
 *     not vectorize.
 * @param legs The reads that find the documents to rerank, never empty, at most one of each type.
 * @param rerank How to rerank the documents and how many to return.
 * @param includeScores The <code>includeScores</code> option of the command.
 * @param includeSortVector The <code>includeSortVector</code> option of the command.
 */
record FindAndRerankPlan(
    VectorizeStep vectorize,
    List<RerankLeg> legs,
    RerankStep rerank,
    boolean includeScores,
    boolean includeSortVector) {

  FindAndRerankPlan {
    // sanity checks, the planner never makes a plan that fails these
    Objects.requireNonNull(legs, "legs must not be null");
    Objects.requireNonNull(rerank, "rerank must not be null");
    if (legs.isEmpty()) {
      throw new IllegalArgumentException("legs must not be empty");
    }
    legs = List.copyOf(legs);

    var legTypes = legs.stream().map(RerankLeg::legType).toList();
    if (legTypes.size() != Set.copyOf(legTypes).size()) {
      throw new IllegalArgumentException(
          "legs must have at most one of each type, legs=" + legTypes);
    }

    // the vectorize step is for a vector leg without a vector, and only for that
    var needsVectorizeStep =
        legs.stream()
            .anyMatch(leg -> leg.legType() == RerankLegType.VECTOR_LEG && leg.vector() == null);
    if (needsVectorizeStep && vectorize == null) {
      throw new IllegalArgumentException(
          "vectorize must not be null when the VECTOR_LEG has no vector");
    }
    if (!needsVectorizeStep && vectorize != null) {
      throw new IllegalArgumentException(
          "vectorize must be null when there is no VECTOR_LEG without a vector");
    }
  }

  /**
   * Vectorize text with the vectorize service of the collection.
   *
   * @param text The text to vectorize.
   * @param dimension The dimension of the vector column of the collection.
   * @param vectorizeDefinition The vectorize service of the collection.
   */
  record VectorizeStep(String text, int dimension, VectorizeDefinition vectorizeDefinition) {

    VectorizeStep {
      Objects.requireNonNull(text, "text must not be null");
      Objects.requireNonNull(vectorizeDefinition, "vectorizeDefinition must not be null");
    }
  }

  /**
   * One read that finds documents to rerank, with the filter of the command and, except for a
   * FILTER_LEG, one sort.
   *
   * @param legType The kind of read.
   * @param vector For a VECTOR_LEG, the vector from the command, or <code>null</code> to use the
   *     vector from the {@link VectorizeStep}. Always <code>null</code> for other legs.
   * @param lexicalText For a LEXICAL_LEG, the text to sort with. Always <code>null</code> for other
   *     legs.
   * @param readLimit The maximum number of documents to read.
   */
  record RerankLeg(RerankLegType legType, float[] vector, String lexicalText, int readLimit) {

    RerankLeg {
      Objects.requireNonNull(legType, "legType must not be null");
      switch (legType) {
        case VECTOR_LEG -> {
          if (lexicalText != null) {
            throw new IllegalArgumentException("lexicalText must be null for a VECTOR_LEG");
          }
        }
        case LEXICAL_LEG -> {
          Objects.requireNonNull(lexicalText, "lexicalText must not be null for a LEXICAL_LEG");
          if (vector != null) {
            throw new IllegalArgumentException("vector must be null for a LEXICAL_LEG");
          }
        }
        case FILTER_LEG -> {
          if (vector != null) {
            throw new IllegalArgumentException("vector must be null for a FILTER_LEG");
          }
          if (lexicalText != null) {
            throw new IllegalArgumentException("lexicalText must be null for a FILTER_LEG");
          }
        }
      }
      if (readLimit < 1) {
        throw new IllegalArgumentException("readLimit must be at least 1, readLimit=" + readLimit);
      }
    }

    /** Override to do a value equality check on the vector */
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof RerankLeg other)
          && legType == other.legType
          && Arrays.equals(vector, other.vector)
          && Objects.equals(lexicalText, other.lexicalText)
          && readLimit == other.readLimit;
    }

    /** Override to do a value equality hash on the vector */
    @Override
    public int hashCode() {
      return Objects.hash(legType, Arrays.hashCode(vector), lexicalText, readLimit);
    }
  }

  /**
   * Rerank the documents the legs read, and return the top documents.
   *
   * @param rerankServiceDef The reranking service, from the command or the collection.
   * @param rerankingQuery The query to rerank with.
   * @param passageLocator Finds the passage to rerank in each document.
   * @param projector The projection for the documents returned.
   * @param limit The maximum number of documents to return.
   */
  record RerankStep(
      CollectionRerankDef.RerankServiceDef rerankServiceDef,
      RerankingQuery rerankingQuery,
      PathMatchLocator passageLocator,
      DocumentProjector projector,
      int limit) {

    RerankStep {
      Objects.requireNonNull(rerankServiceDef, "rerankServiceDef must not be null");
      Objects.requireNonNull(rerankingQuery, "rerankingQuery must not be null");
      Objects.requireNonNull(passageLocator, "passageLocator must not be null");
      Objects.requireNonNull(projector, "projector must not be null");
      if (limit < 1) {
        throw new IllegalArgumentException("limit must be at least 1, limit=" + limit);
      }
    }
  }
}
