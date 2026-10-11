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
 * <p>Only {@link #HYBRID} can be run for now. The sorts that read with a single <code>$vectorize
 * </code>, <code>$vector</code> or <code>$lexical</code> key get their own modes when the parser
 * accepts them (GH #2601, GH #2602).
 */
public enum SortMode {
  /**
   * A <code>$hybrid</code> sort. The planner runs a vector or vectorize leg and / or a lexical leg,
   * depending on the values in the sort and the collection.
   */
  HYBRID,
  /**
   * No sort, or an empty sort. The planner rejects it with UNSUPPORTED_FIND_AND_RERANK_SORT until
   * findAndRerank can run a read with only the filter (GH #2603).
   */
  FILTER
}
