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
  ADA002("ada002", SimilarityFunction.DOT_PRODUCT),
  BERT("bert", SimilarityFunction.DOT_PRODUCT),
  COHERE_V3("cohere-v3", SimilarityFunction.DOT_PRODUCT),
  GECKO("gecko", SimilarityFunction.DOT_PRODUCT),
  NV_QA_4("nv-qa-4", SimilarityFunction.DOT_PRODUCT),
  OPENAI_V3_LARGE("openai-v3-large", SimilarityFunction.DOT_PRODUCT),
  OPENAI_V3_SMALL("openai-v3-small", SimilarityFunction.DOT_PRODUCT),
  OTHER("other", SimilarityFunction.COSINE),
  // NOTE: was null originally for undefined
  UNDEFINED("undefined", null);

  // TODO: Add a comment why this is the default
  public static final EmbeddingSourceModel DEFAULT_SOURCE_MODEL = OTHER;

  private final String name;
  private final SimilarityFunction similarityFunction;

  private static final Map<String, EmbeddingSourceModel> SOURCE_MODEL_BY_NAME =
      Arrays.stream(EmbeddingSourceModel.values())
          .collect(Collectors.toMap(model -> model.name().toLowerCase(), Function.identity()));

  EmbeddingSourceModel(String name, SimilarityFunction similarityFunction) {
    this.name = name;
    this.similarityFunction = similarityFunction;
  }

  public String getName() {
    return name;
  }

  /**
   * Get the similarity function for the source model.
   *
   * @return null if there is no default similarity function, otherwise the similarity function
   */
  public SimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public static String getSupportedSourceModelNames() {
    return Arrays.stream(EmbeddingSourceModel.values())
        .filter(v -> !v.equals(EmbeddingSourceModel.UNDEFINED))
        .map(EmbeddingSourceModel::getName)
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

  /**
   * Converts a string representation of a source model name to its corresponding {@link
   * EmbeddingSourceModel} enum.
   *
   * <p>If the provided name is <code>null</code>, returns {@link EmbeddingSourceModel#UNDEFINED},
   * used for non-vector collections. If the name is an empty string, returns the default {@link
   * EmbeddingSourceModel#DEFAULT_SOURCE_MODEL}, indicating the collection was created before source
   * models were supported.
   *
   * @param name Name of the source model
   * @return Optional with the corresponding {@link EmbeddingSourceModel} or empty if the name is
   *     not known
   */
  public static Optional<EmbeddingSourceModel> fromName(String name) {
    return switch (name) {
      case null -> Optional.of(UNDEFINED);
      case String s when s.isBlank() -> Optional.of(OTHER);
      default -> Optional.ofNullable(SOURCE_MODEL_BY_NAME.get(name.toLowerCase()));
    };
  }

  /**
   * HACK - aaron nov 11 2024, this used to be in the fromName method which it should not be because
   * the caller should decide when to throw. Moved to be a public method here so it can be used by
   * the caller.
   */
  public static JsonApiException getUnknownSourceModelException(String modelName) {
    return ErrorCodeV1.VECTOR_SEARCH_UNRECOGNIZED_SOURCE_MODEL_NAME.toApiException(
        "Received: '%s'; Accepted: %s", modelName, getSupportedSourceModelNames());
  }
}
