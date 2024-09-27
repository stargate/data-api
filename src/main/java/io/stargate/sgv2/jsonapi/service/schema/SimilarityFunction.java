package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;

/**
 * The similarity function used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum SimilarityFunction {
  COSINE,
  EUCLIDEAN,
  DOT_PRODUCT,
  UNDEFINED;

  // TODO: store the name of the enum in the enum itself
  public static SimilarityFunction fromString(String similarityFunction) {
    if (similarityFunction == null) return UNDEFINED;
    return switch (similarityFunction.toLowerCase()) {
      case "cosine" -> COSINE;
      case "euclidean" -> EUCLIDEAN;
      case "dot_product" -> DOT_PRODUCT;
      default ->
          throw ErrorCodeV1.VECTOR_SEARCH_INVALID_FUNCTION_NAME.toApiException(
              "'%s'", similarityFunction);
    };
  }
}
