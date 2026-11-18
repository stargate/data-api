package io.stargate.sgv2.jsonapi.config.constants;

import java.util.Set;

public interface VectorConstants {
  interface VectorColumn {
    String DIMENSION = "dimension";
    String METRIC = "metric";
    String SOURCE_MODEL = "sourceModel";
    String SERVICE = ServiceDescConstants.SERVICE;
    String VECTOR_INDEXING = "vectorIndexing";
  }

  interface Vectorize extends ServiceDescConstants {}

  /**
   * CQL {@code WITH OPTIONS} keys for a vector (SAI) index. {@link #SOURCE_MODEL} and {@link
   * #SIMILARITY_FUNCTION} map to dedicated API fields ({@code sourceModel} / {@code metric}); the
   * rest are tuning options set via {@code vectorIndexing.options}.
   */
  interface CQLAnnIndex {
    String SOURCE_MODEL = "source_model";
    String SIMILARITY_FUNCTION = "similarity_function";
    String MAXIMUM_NODE_CONNECTIONS = "maximum_node_connections";
    String CONSTRUCTION_BEAM_WIDTH = "construction_beam_width";
    String NEIGHBORHOOD_OVERFLOW = "neighborhood_overflow";
    String ALPHA = "alpha";
    String ENABLE_HIERARCHY = "enable_hierarchy";

    /**
     * Options with dedicated API fields ({@code metric} / {@code sourceModel}); rejected inside
     * {@code vectorIndexing.options}.
     */
    Set<String> RESERVED_OPTIONS = Set.of(SOURCE_MODEL, SIMILARITY_FUNCTION);

    /**
     * SAI tuning options settable through {@code vectorIndexing.options}. {@code optimize_for}
     * exists in OSS Cassandra but is de-emphasized in DSE 6.9 / HCD, so it is left out for now.
     */
    Set<String> ALLOWED_OPTIONS =
        Set.of(
            MAXIMUM_NODE_CONNECTIONS,
            CONSTRUCTION_BEAM_WIDTH,
            NEIGHBORHOOD_OVERFLOW,
            ALPHA,
            ENABLE_HIERARCHY);

    /** Allowed options whose value must be a boolean; the rest are numeric. */
    Set<String> BOOLEAN_OPTIONS = Set.of(ENABLE_HIERARCHY);
  }
}
