package io.stargate.sgv2.jsonapi.config.constants;

import io.smallrye.config.ConfigMapping;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.Map;

@ConfigMapping(prefix = "stargate.jsonapi.vector")
public interface VectorConstant {
  /*
  Supported Source Models and suggested function for Vector Index in Cassandra
   */
  Map<String, SimilarityFunction> SUPPORTED_SOURCES =
      Map.of(
          "ada002",
          SimilarityFunction.DOT_PRODUCT,
          "openai-v3-small",
          SimilarityFunction.DOT_PRODUCT,
          "openai-v3-large",
          SimilarityFunction.DOT_PRODUCT,
          "bert",
          SimilarityFunction.DOT_PRODUCT,
          "gecko",
          SimilarityFunction.DOT_PRODUCT,
          "nv-qa-4",
          SimilarityFunction.DOT_PRODUCT,
          "cohere-v3",
          SimilarityFunction.DOT_PRODUCT,
          "other",
          SimilarityFunction.COSINE);
}
