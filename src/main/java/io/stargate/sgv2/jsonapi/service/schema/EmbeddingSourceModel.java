package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The source model used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum EmbeddingSourceModel {
  ADA002("ada002", "ADA002", SimilarityFunction.DOT_PRODUCT),
  BERT("bert", "BERT", SimilarityFunction.DOT_PRODUCT),
  COHERE_V3("cohere-v3", "COHERE_V3", SimilarityFunction.DOT_PRODUCT),
  GECKO("gecko", "GECKO", SimilarityFunction.DOT_PRODUCT),
  NV_QA_4("nv-qa-4", "NV_QA_4", SimilarityFunction.DOT_PRODUCT),
  OPENAI_V3_LARGE("openai-v3-large", "OPENAI_V3_LARGE", SimilarityFunction.DOT_PRODUCT),
  OPENAI_V3_SMALL("openai-v3-small", "OPENAI_V3_SMALL", SimilarityFunction.DOT_PRODUCT),
  OTHER("other", "OTHER", SimilarityFunction.COSINE),
  // NOTE: was null originally for undefined
  UNDEFINED("undefined", "undefined", null);

  // TODO: Add a comment why this is the default
  public static final EmbeddingSourceModel DEFAULT = OTHER;

  private final String apiName;
  private final String cqlName;
  private final SimilarityFunction similarityFunction;

  private static final Map<String, EmbeddingSourceModel> SOURCE_MODEL_BY_API_NAME =
      Arrays.stream(EmbeddingSourceModel.values())
          .collect(Collectors.toMap(model -> model.apiName().toLowerCase(), Function.identity()));

  private static final Map<String, EmbeddingSourceModel> SOURCE_MODEL_BY_CQL_NAME =
      Arrays.stream(EmbeddingSourceModel.values())
          .collect(Collectors.toMap(model -> model.cqlName().toLowerCase(), Function.identity()));

  EmbeddingSourceModel(String apiName, String cqlName, SimilarityFunction similarityFunction) {
    this.apiName = apiName;
    this.cqlName = cqlName;
    this.similarityFunction = similarityFunction;
  }

  public String cqlName() {
    return cqlName;
  }

  public String apiName() {
    return apiName;
  }

  /**
   * Get the similarity function for the source model.
   *
   * @return null if there is no default similarity function, otherwise the similarity function
   */
  public SimilarityFunction similarityFunction() {
    return similarityFunction;
  }

  public static String getSupportedSourceModelNames() {
    return Arrays.stream(EmbeddingSourceModel.values())
        .filter(v -> !v.equals(EmbeddingSourceModel.UNDEFINED))
        .map(EmbeddingSourceModel::apiName)
        .collect(Collectors.joining(", "));
  }

  /**
   * Get the recommended similarity function for the given source model.
   *
   * @param sourceModel The source model
   * @return The similarity function
   */
  //  public static SimilarityFunction getSimilarityFunction(EmbeddingSourceModel sourceModel) {
  //    return SOURCE_MODEL_METRIC_MAP.get(sourceModel);
  //  }

  /**
   * Get the recommended similarity function for the given source model name.
   *
   * @param sourceModelName The source model name
   * @return The similarity function
   */
  //  public static SimilarityFunction getSimilarityFunction(String sourceModelName) {
  //    return SOURCE_MODEL_NAME_MAP.get(sourceModelName) == null
  //        ? null
  //        : getSimilarityFunction(SOURCE_MODEL_NAME_MAP.get(sourceModelName));
  //  }

  public static Optional<EmbeddingSourceModel> fromCqlName(String cqlName) {
    return cqlName == null
        ? Optional.empty()
        : Optional.ofNullable(SOURCE_MODEL_BY_CQL_NAME.get(cqlName.toLowerCase()));
  }

  public static Optional<EmbeddingSourceModel> fromCqlNameOrDefault(String cqlName) {
    return switch (cqlName) {
      case null -> Optional.of(DEFAULT);
      case String s when s.isBlank() -> Optional.of(DEFAULT);
      default -> fromCqlName(cqlName);
    };
  }

  /**
   * Converts a string representation of a source model apiName to its corresponding {@link
   * EmbeddingSourceModel} enum if it exists.
   *
   * @param apiName Name of the source model, there is no default returned. Nullable.
   * @return Optional with the corresponding {@link EmbeddingSourceModel} or empty if the apiName is
   *     not known, there are no defaults returned. Use {@link #DEFAULT}.
   */
  public static Optional<EmbeddingSourceModel> fromApiName(String apiName) {
    return apiName == null
        ? Optional.empty()
        : Optional.ofNullable(SOURCE_MODEL_BY_API_NAME.get(apiName.toLowerCase()));
  }

  /**
   * Like {@link #fromApiName(String)} but if the apiName is null or blank it will return the
   * default.
   *
   * @param apiName The apiName of the source model
   * @return Optional with the corresponding {@link EmbeddingSourceModel}, or {@link #DEFAULT} if
   *     the apiName is null or blank, or empty if the apiName is not known
   */
  public static Optional<EmbeddingSourceModel> fromApiNameOrDefault(String apiName) {
    return switch (apiName) {
      case null -> Optional.of(DEFAULT);
      case String s when s.isBlank() -> Optional.of(DEFAULT);
      default -> fromApiName(apiName);
    };
  }

  /**
   * HACK - aaron nov 11 2024, this used to be in the fromName method which it should not be because
   * the caller should decide when to throw. Moved to be a public method here so it can be used by
   * the caller.
   */
  public static JsonApiException getUnknownSourceModelException(String apiName) {
    return ErrorCodeV1.VECTOR_SEARCH_UNRECOGNIZED_SOURCE_MODEL_NAME.toApiException(
        "Received: '%s'; Accepted: %s", apiName, getSupportedSourceModelNames());
  }
}
