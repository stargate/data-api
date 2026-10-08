package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

/**
 * The kind of retrieval the user asked for in the sort of a findAndRerank command.
 *
 * <p>The mode is decided when the sort is parsed, and only from which keys are in the sort. It is
 * what the user asked for, not the list of reads (legs) that will run: the planner combines the
 * mode with the collection schema to decide the legs. For example, <code>{"$hybrid": "cheese"}
 * </code> is always {@link #HYBRID}, it runs a vectorize leg and a lexical leg when the collection
 * has a lexical index, and only the vectorize leg when it does not.
 *
 * <p>Only {@link #HYBRID} is supported for now. The parser does not produce VECTORIZE, VECTOR or
 * LEXICAL yet, and the planner rejects every mode other than HYBRID with
 * UNSUPPORTED_FIND_AND_RERANK_SORT.
 */
public enum LegMode {
  /** A <code>$hybrid</code> sort, runs a vector or vectorize leg and / or a lexical leg. */
  HYBRID,
  /** A <code>$vectorize</code> sort, runs one vectorize leg. */
  VECTORIZE,
  /** A <code>$vector</code> sort, runs one vector leg. */
  VECTOR,
  /** A <code>$lexical</code> sort, runs one lexical leg. */
  LEXICAL,
  /** No sort, or an empty sort, runs one plain filter read. */
  FILTER
}
