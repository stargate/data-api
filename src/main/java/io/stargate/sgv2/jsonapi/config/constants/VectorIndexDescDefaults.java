package io.stargate.sgv2.jsonapi.config.constants;

import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;

public interface VectorIndexDescDefaults {

  // SEE ALSO SimilarityFunction and ApiVectorIndex
  String DEFAULT_METRIC_NAME = SimilarityFunction.ApiConstants.COSINE;
}
