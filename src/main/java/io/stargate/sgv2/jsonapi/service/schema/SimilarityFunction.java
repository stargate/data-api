package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The similarity function used for the vector index. This is only applicable if the vector index is
 * enabled.
 *
 * <p>For indexing and projection functions see
 * https://cassandra.apache.org/doc/latest/cassandra/getting-started/vector-search-quickstart.html
 */
public enum SimilarityFunction {
  COSINE("cosine", "COSINE", "similarity_cosine"),
  EUCLIDEAN("euclidean", "EUCLIDEAN", "similarity_euclidean"),
  DOT_PRODUCT("dot_product", "DOT_PRODUCT", "similarity_dot_product"),
  UNDEFINED("undefined", "undefined", "undefined");

  public static final SimilarityFunction DEFAULT_SIMILARITY_FUNCTION = COSINE;

  private String cqlProjectionFunction;
  private String cqlIndexingFunction;
  private String apiFunctionName;

  private static Map<String, SimilarityFunction> FUNCTION_BY_INDEXING_FUNCTION = new HashMap<>();
  private static Map<String, SimilarityFunction> FUNCTION_BY_API_NAME = new HashMap<>();

  static {
    for (SimilarityFunction similarityFunction : SimilarityFunction.values()) {
      if (similarityFunction != UNDEFINED) {

        FUNCTION_BY_INDEXING_FUNCTION.put(
            similarityFunction.cqlIndexingFunction().toLowerCase(), similarityFunction);
        FUNCTION_BY_API_NAME.put(
            similarityFunction.apiFunctionName.toLowerCase(), similarityFunction);
      }
    }
  }

  SimilarityFunction(
      String apiFunctionName, String cqlIndexingFunction, String cqlProjectionFunction) {
    this.apiFunctionName = apiFunctionName;
    this.cqlIndexingFunction = cqlIndexingFunction;
    this.cqlProjectionFunction = cqlProjectionFunction;
  }

  // TODO AARON - removed the @JsonValue becaue this should not be exposed to the user
  //  @JsonValue
  public String cqlProjectionFunction() {
    return cqlProjectionFunction;
  }

  public String cqlIndexingFunction() {
    return cqlIndexingFunction;
  }

  public String apiName() {
    return apiFunctionName;
  }

  public static Optional<SimilarityFunction> fromCqlIndexingFunction(String cqlIndexingFunction) {
    return fromName(cqlIndexingFunction, FUNCTION_BY_INDEXING_FUNCTION);
  }

  public static Optional<SimilarityFunction> fromApiName(String apiName) {
    return fromName(apiName, FUNCTION_BY_API_NAME);
  }

  private static Optional<SimilarityFunction> fromName(
      String name, Map<String, SimilarityFunction> map) {

    // not standard behavior, but the code already did this, would be better to have the enum throw
    // or return null and then the caller work out what to do. - aaron
    if (name == null || name.isBlank()) {
      return Optional.of(UNDEFINED);
    }

    return Optional.ofNullable(map.get(name.toLowerCase()));
  }

  /**
   * HACK - aaron 11 nov, it used to be that getting hte function from the string name would throw
   * this if it was not found, changed so that returns optional so the called can work out what to
   * do. Call this to get the exception
   */
  public static JsonApiException getUnknownFunctionException(String functionName) {
    return ErrorCodeV1.VECTOR_SEARCH_INVALID_FUNCTION_NAME.toApiException("'%s'", functionName);
  }
}
