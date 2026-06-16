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
   * Names of the options used in the CQL {@code CREATE CUSTOM INDEX ... WITH OPTIONS = {...}}
   * clause for a vector (SAI ANN) index. {@link #SOURCE_MODEL} and {@link #SIMILARITY_FUNCTION}
   * have dedicated API fields ({@code sourceModel} / {@code metric}); the remaining tuning options
   * are exposed via the {@code vectorIndexing} field (see {@link VectorColumn#VECTOR_INDEXING}).
   */
  interface CQLAnnIndex {
    String SOURCE_MODEL = "source_model";
    String SIMILARITY_FUNCTION = "similarity_function";
    String MAXIMUM_NODE_CONNECTIONS = "maximum_node_connections";
    String CONSTRUCTION_BEAM_WIDTH = "construction_beam_width";

    /**
     * Options that have dedicated API fields ({@code sourceModel} / {@code metric}) and so must not
     * be set again through the raw {@code vectorIndexing} object.
     */
    Set<String> RESERVED_OPTIONS = Set.of(SOURCE_MODEL, SIMILARITY_FUNCTION);
  }
}
