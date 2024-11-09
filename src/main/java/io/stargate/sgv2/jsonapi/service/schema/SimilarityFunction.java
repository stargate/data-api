package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.HashMap;
import java.util.Map;

/**
 * The similarity function used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum SimilarityFunction {
  COSINE("cosine", "cosine"),
  EUCLIDEAN("euclidean", "euclidean"),
  DOT_PRODUCT("dot_product", "dot_product"),
  UNDEFINED("undefined", "undefined");

  public static final SimilarityFunction DEFAULT_SIMILARITY_FUNCTION = COSINE;

  private String cqlFunctionName;
  private String apiFunctionName;

  private static Map<String, SimilarityFunction> FUNCTION_BY_CQL_NAME = new HashMap<>();
  private static Map<String, SimilarityFunction> FUNCTION_BY_API_NAME = new HashMap<>();

  static {
    for (SimilarityFunction similarityFunction : SimilarityFunction.values()) {
      FUNCTION_BY_CQL_NAME.put(similarityFunction.getCqlName(), similarityFunction);
      FUNCTION_BY_API_NAME.put(similarityFunction.getApiName(), similarityFunction);
    }
  }

  SimilarityFunction(String cqlFunctionName, String apiFunctionName) {
    this.cqlFunctionName = cqlFunctionName;
    this.apiFunctionName = apiFunctionName;
  }

  // TODO AARON - removed the @JsonValue becaue this should not be exposed to the user
  //  @JsonValue
  public String getCqlName() {
    return cqlFunctionName;
  }

  public String getApiName() {
    return apiFunctionName;
  }

  public static SimilarityFunction fromCqlName(String cqlName) {
    return fromName(cqlName, FUNCTION_BY_CQL_NAME);
  }

  public static SimilarityFunction fromApiName(String apiName) {
    return fromName(apiName, FUNCTION_BY_API_NAME);
  }

  private static SimilarityFunction fromName(String name, Map<String, SimilarityFunction> map) {

    // not standard behavior, but the code already did this, would be better to have the enum throw
    // or return null and then the caller work out what to do.
    if (name == null) {
      return UNDEFINED;
    }

    var function = map.get(name);
    if (function == null) {
      throw ErrorCodeV1.VECTOR_SEARCH_INVALID_FUNCTION_NAME.toApiException("'%s'", name);
    }
    return function;
  }
}
