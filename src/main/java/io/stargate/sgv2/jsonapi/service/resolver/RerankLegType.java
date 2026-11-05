package io.stargate.sgv2.jsonapi.service.resolver;

/** The kind of read a {@link FindAndRerankPlan.RerankLeg} does. */
enum RerankLegType {
  /** Reads with a vector sort, the vector is from the command or from vectorizing text. */
  VECTOR_LEG,
  /** Reads with a lexical (BM25) sort. */
  LEXICAL_LEG,
  /**
   * Reads with only the filter of the command, no sort.
   *
   * <p>TODO: GH #2603 - the planner does not make a FILTER_LEG yet, and the builder does not read
   * with one.
   */
  FILTER_LEG
}
