package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

/**
 * The kind of sort the user wrote in a findAndRerank command.
 *
 * <p>The mode is decided when the sort is parsed, and only from which keys are in the sort. It is
 * the sort the user asked for, not the reads (legs) that will run: the planner combines the mode
 * with the sort values and the collection schema to decide the legs. For example, <code>
 * {"$hybrid": "cheese"}</code> is always {@link #HYBRID}, it runs a vectorize leg and a lexical leg
 * when the collection has a lexical index, and only the vectorize leg when it does not.
 *
 * <p>Only {@link #HYBRID} can be run for now, the planner rejects every other mode with
 * UNSUPPORTED_FIND_AND_RERANK_SORT.
 *
 * <p>TODO: GH #2602 - the parser does not produce {@link #VECTORIZE}, {@link #VECTOR} or {@link
 * #LEXICAL} yet, they will be implemented when findAndRerank supports single leg sorts.
 */
public enum LegMode {
  /**
   * A <code>$hybrid</code> sort. The planner runs a vector or vectorize leg and / or a lexical leg,
   * depending on the values in the sort and the collection.
   */
  HYBRID,
  /** A <code>$vectorize</code> sort, runs one vectorize leg. */
  VECTORIZE,
  /** A <code>$vector</code> sort, runs one vector leg. */
  VECTOR,
  /** A <code>$lexical</code> sort, runs one lexical leg. */
  LEXICAL,
  /**
   * No sort, or an empty sort. The planner rejects it with UNSUPPORTED_FIND_AND_RERANK_SORT until
   * findAndRerank can run a read with only the filter (GH #2603).
   */
  FILTER
}
