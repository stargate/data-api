package io.stargate.sgv2.jsonapi.config.constants;

import io.smallrye.config.ConfigMapping;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.Map;

@ConfigMapping(prefix = "stargate.jsonapi.vector")
public interface VectorConstants {
  /*
  Supported Source Models and suggested function for Vector Index in Cassandra
   */
  Map<String, SimilarityFunction> SUPPORTED_SOURCES =
      Map.of(
          "ada002",
          SimilarityFunction.DOT_PRODUCT,
          "openai_v3_small",
          SimilarityFunction.DOT_PRODUCT,
          "openai_v3_large",
          SimilarityFunction.DOT_PRODUCT,
          "bert",
          SimilarityFunction.DOT_PRODUCT,
          "gecko",
          SimilarityFunction.DOT_PRODUCT,
          "nv_qa_4",
          SimilarityFunction.DOT_PRODUCT,
          "cohere_v3",
          SimilarityFunction.DOT_PRODUCT,
          "other",
          SimilarityFunction.COSINE);

  interface VectorColumn {
    String DIMENSION = "dimension";
    String METRIC = "metric";
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
