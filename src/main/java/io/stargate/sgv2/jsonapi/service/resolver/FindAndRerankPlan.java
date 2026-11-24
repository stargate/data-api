package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.reranking.Rank;
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
 * VectorizeStep}, run each {@link ReadPlan}, then rerank what the reads find with the {@link
 * RerankStep}. {@link FindAndRerankOperationBuilder} builds the tasks from it.
 *
 * <p><b>NOTE:</b> create new plans with {@link FindAndRerankPlanner#plan}, that is where the
 * command is validated. The checks in the constructors of these records are sanity checks that the
 * code making a plan is correct, they do not validate the command.
 *
 * @param vectorizeStep The vectorize step for the {@link VectorizeReadPlan}, <code>null</code> when
 *     the command does not vectorize.
 * @param reads The reads that find the documents to rerank, never empty, at most one for each rank
 *     source.
 * @param rerankStep How to rerank the documents and how many to return.
 * @param includeScores The <code>includeScores</code> option of the command.
 * @param includeSortVector The <code>includeSortVector</code> option of the command.
 */
record FindAndRerankPlan(
    VectorizeStep vectorizeStep,
    List<ReadPlan> reads,
    RerankStep rerankStep,
    // TODO: move includeScores, includeSortVector and the projector of the RerankStep into a
    // ProjectionStep
    boolean includeScores,
    boolean includeSortVector) {

  FindAndRerankPlan {
    // sanity checks, the planner never makes a plan that fails these
    Objects.requireNonNull(reads, "reads must not be null");
    Objects.requireNonNull(rerankStep, "rerankStep must not be null");
    if (reads.isEmpty()) {
      throw new IllegalArgumentException("reads must not be empty");
    }
    reads = List.copyOf(reads);

    // all the checks below are sanity checks to make sure the plan is valid, not the user input
    // validation

    // at most one read for each rank source, so a document has at most one rank from each source
    var rankSources = reads.stream().map(ReadPlan::rankSource).toList();
    if (rankSources.size() != Set.copyOf(rankSources).size()) {
      throw new IllegalArgumentException(
          "reads must have at most one read for each rank source, rankSources=" + rankSources);
    }

    // the vectorize step is for a VectorizeReadPlan, and only for that
    var needsVectorizeStep = reads.stream().anyMatch(read -> read instanceof VectorizeReadPlan);
    if (needsVectorizeStep && vectorizeStep == null) {
      throw new IllegalArgumentException(
          "vectorizeStep must not be null when there is a VectorizeReadPlan");
    }
    if (!needsVectorizeStep && vectorizeStep != null) {
      throw new IllegalArgumentException(
          "vectorizeStep must be null when there is no VectorizeReadPlan");
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
   * One read that finds documents to rerank, with the filter of the command and the sort of the
   * read. There is one record for each kind of read.
   */
  sealed interface ReadPlan permits VectorReadPlan, VectorizeReadPlan, LexicalReadPlan {

    /** The maximum number of documents to read. */
    int readLimit();

    /** The source of the ranks of the documents the read finds. */
    Rank.RankSource rankSource();
  }

  /**
   * A read with a vector sort, the vector is from the command.
   *
   * @param vector The vector to sort with.
   * @param readLimit The maximum number of documents to read.
   */
  record VectorReadPlan(float[] vector, int readLimit) implements ReadPlan {

    VectorReadPlan {
      // sanity checks, the planner never makes a read that fails these
      Objects.requireNonNull(vector, "vector must not be null");
      checkReadLimit(readLimit);
    }

    @Override
    public Rank.RankSource rankSource() {
      return Rank.RankSource.VECTOR;
    }

    /** Override to do a value equality check on the vector */
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof VectorReadPlan other)
          && Arrays.equals(vector, other.vector)
          && readLimit == other.readLimit;
    }

    /** Override to do a value equality hash on the vector */
    @Override
    public int hashCode() {
      return Objects.hash(Arrays.hashCode(vector), readLimit);
    }
  }

  /**
   * A read with a vector sort, the vector is from the {@link VectorizeStep} of the plan.
   *
   * @param readLimit The maximum number of documents to read.
   */
  record VectorizeReadPlan(int readLimit) implements ReadPlan {

    VectorizeReadPlan {
      // sanity check, the planner never makes a read that fails it
      checkReadLimit(readLimit);
    }

    @Override
    public Rank.RankSource rankSource() {
      return Rank.RankSource.VECTOR;
    }
  }

  /**
   * A read with a lexical (BM25) sort.
   *
   * @param lexicalText The text to sort with.
   * @param readLimit The maximum number of documents to read.
   */
  record LexicalReadPlan(String lexicalText, int readLimit) implements ReadPlan {

    LexicalReadPlan {
      // sanity checks, the planner never makes a read that fails these
      Objects.requireNonNull(lexicalText, "lexicalText must not be null");
      checkReadLimit(readLimit);
    }

    @Override
    public Rank.RankSource rankSource() {
      return Rank.RankSource.BM25;
    }
  }

  /**
   * Rerank the documents the reads find, and return the top documents.
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

  /** Checks the read limit of a {@link ReadPlan} is at least 1. */
  private static void checkReadLimit(int readLimit) {
    if (readLimit < 1) {
      throw new IllegalArgumentException("readLimit must be at least 1, readLimit=" + readLimit);
    }
  }
}
