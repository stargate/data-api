package io.stargate.sgv2.jsonapi.config.constants;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "stargate.jsonapi.vector")
public interface VectorConstants {
  interface VectorColumn {
    String DIMENSION = "dimension";
    String METRIC = "metric";
    String SOURCE_MODEL = "sourceModel";
    String SERVICE = "service";
  }

  interface Vectorize {
    String PROVIDER = "provider";
    String MODEL_NAME = "modelName";
    String AUTHENTICATION = "authentication";
    String PARAMETERS = "parameters";
  }

  interface CQLAnnIndex {
    String SOURCE_MODEL = "source_model";
    String SIMILARITY_FUNCTION = "similarity_function";
  }
}
