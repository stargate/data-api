package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.HashMap;
import java.util.Map;

/**
 * The similarity function used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum SimilarityFunction {
  COSINE("cosine"),
  EUCLIDEAN("euclidean"),
  DOT_PRODUCT("dot_product"),
  UNDEFINED("undefined");

  private String metric;
  private static Map<String, SimilarityFunction> FUNCTIONS_MAP = new HashMap<>();

  static {
    for (SimilarityFunction similarityFunction : SimilarityFunction.values()) {
      FUNCTIONS_MAP.put(similarityFunction.getMetric(), similarityFunction);
    }
  }

  private SimilarityFunction(String metric) {
    this.metric = metric;
  }

  public String getMetric() {
    return metric;
  }

  // TODO: store the name of the enum in the enum itself
  public static SimilarityFunction fromString(String similarityFunction) {
    if (similarityFunction == null) return UNDEFINED;
    SimilarityFunction function = FUNCTIONS_MAP.get(similarityFunction);
    if (function == null) {
      throw ErrorCodeV1.VECTOR_SEARCH_INVALID_FUNCTION_NAME.toApiException(
          "'%s'", similarityFunction);
    }
    return function;
  }
}
