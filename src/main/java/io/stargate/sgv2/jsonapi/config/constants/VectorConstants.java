package io.stargate.sgv2.jsonapi.config.constants;

public interface VectorConstants {
  interface VectorColumn {
    String DIMENSION = "dimension";
    String METRIC = "metric";
    String SOURCE_MODEL = "sourceModel";
    String SERVICE = ServiceDescConstants.SERVICE;
  }

  interface Vectorize extends ServiceDescConstants {}

  interface CQLAnnIndex {
    String SOURCE_MODEL = "source_model";
    String SIMILARITY_FUNCTION = "similarity_function";
  }
}
