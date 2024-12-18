package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
  DOT_PRODUCT("dot_product", "DOT_PRODUCT", "similarity_dot_product");

  public static final SimilarityFunction DEFAULT = COSINE;

  private String cqlProjectionFunction;
  private String cqlIndexingFunction;
  private String apiFunctionName;

  private static Map<String, SimilarityFunction> FUNCTION_BY_INDEXING_FUNCTION = new HashMap<>();
  private static Map<String, SimilarityFunction> FUNCTION_BY_API_NAME = new HashMap<>();

  static {
    for (SimilarityFunction similarityFunction : SimilarityFunction.values()) {
      FUNCTION_BY_INDEXING_FUNCTION.put(
          similarityFunction.cqlIndexingFunction().toLowerCase(), similarityFunction);
      FUNCTION_BY_API_NAME.put(
          similarityFunction.apiFunctionName.toLowerCase(), similarityFunction);
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

  /** The CQL select function, such as `similarity_cosine` */
  public String cqlProjectionFunction() {
    return cqlProjectionFunction;
  }

  /** The CQL indexing function, this is the `similarity_function` in the index options */
  public String cqlIndexingFunction() {
    return cqlIndexingFunction;
  }

  /** The name we use for this similarity function in the API */
  public String apiName() {
    return apiFunctionName;
  }

  /**
   * Decides on the similarity function to use based on the user input or the model.
   *
   * @param userProvided Flag true if the user provided a name of the function to use.
   * @param userOrDefaultFunction The function that was mapped from the user input, or or null or
   *     the default if the user did not provide a function.
   * @param sourceModel The source model that either the user selected, or the default. Not null.
   * @return The similarity function to use, either the one from the user or the model.
   */
  public static SimilarityFunction decideFromInputOrModel(
      boolean userProvided,
      SimilarityFunction userOrDefaultFunction,
      EmbeddingSourceModel sourceModel) {
    Objects.requireNonNull(sourceModel, "sourceModel");

    if (!userProvided) {
      // the user did not provide a function, so we use the one from the model
      return sourceModel.similarityFunction();
    }
    // the user provided a function, so we use that one
    return userOrDefaultFunction == null ? DEFAULT : userOrDefaultFunction;
  }

  /**
   * Gets the {@link SimilarityFunction} with the specified {@link #cqlIndexingFunction}
   *
   * @param cqlIndexingFunction the CQL indexing function, this is the `similarity_function` in the
   *     index options
   * @return Optional of {@link SimilarityFunction} or {@link Optional#empty()} if not found
   */
  public static Optional<SimilarityFunction> fromCqlIndexingFunction(String cqlIndexingFunction) {
    return cqlIndexingFunction == null
        ? Optional.empty()
        : Optional.ofNullable(FUNCTION_BY_INDEXING_FUNCTION.get(cqlIndexingFunction.toLowerCase()));
  }

  /**
   * Gets the {@link SimilarityFunction} with the specified {@link #apiFunctionName}
   *
   * @param apiName the name we use for this similarity function in the API
   * @return Optional of {@link SimilarityFunction} or {@link Optional#empty()} if the name is null
   *     or blank or not found
   */
  public static Optional<SimilarityFunction> fromApiName(String apiName) {
    return apiName == null
        ? Optional.empty()
        : Optional.ofNullable(FUNCTION_BY_API_NAME.get(apiName.toLowerCase()));
  }

  /**
   * Gets the {@link SimilarityFunction} with the specified {@link #apiFunctionName} but with a
   * default.
   *
   * @param apiName the name we use for this similarity function in the API
   * @return Optional of {@link SimilarityFunction} , or with the {@link #DEFAULT} if the name is
   *     null or blank , or {@link Optional#empty()} if not found
   */
  public static Optional<SimilarityFunction> fromApiNameOrDefault(String apiName) {
    return (apiName == null || apiName.isBlank()) ? Optional.of(DEFAULT) : fromApiName(apiName);
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
