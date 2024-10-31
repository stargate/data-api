package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The source model used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum SourceModel {
  ADA002("ada002"),
  BERT("bert"),
  COHERE_V3("cohere-v3"),
  GECKO("gecko"),
  NV_QA_4("nv-qa-4"),
  OPENAI_V3_LARGE("openai-v3-large"),
  OPENAI_V3_SMALL("openai-v3-small"),
  OTHER("other"),
  UNDEFINED("undefined");

  private final String name;
  public static final Map<String, SourceModel> SOURCE_MODEL_NAME_MAP =
      Stream.of(SourceModel.values())
          .collect(Collectors.toMap(SourceModel::getName, sourceModel -> sourceModel));

  /** Supported Source Models and suggested similarity function for Vector Index in Cassandra */
  private static final Map<SourceModel, SimilarityFunction> SOURCE_MODEL_METRIC_MAP =
      Map.of(
          ADA002,
          SimilarityFunction.DOT_PRODUCT,
          BERT,
          SimilarityFunction.DOT_PRODUCT,
          COHERE_V3,
          SimilarityFunction.DOT_PRODUCT,
          GECKO,
          SimilarityFunction.DOT_PRODUCT,
          NV_QA_4,
          SimilarityFunction.DOT_PRODUCT,
          OPENAI_V3_LARGE,
          SimilarityFunction.DOT_PRODUCT,
          OPENAI_V3_SMALL,
          SimilarityFunction.DOT_PRODUCT,
          OTHER,
          SimilarityFunction.COSINE);

  public static final SourceModel DEFAULT_SOURCE_MODEL = OTHER;

  SourceModel(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  /**
   * Get the recommended similarity function for the given source model.
   *
   * @param sourceModel The source model
   * @return The similarity function
   */
  public static SimilarityFunction getSimilarityFunction(SourceModel sourceModel) {
    return SOURCE_MODEL_METRIC_MAP.get(sourceModel);
  }

  /**
   * Get the recommended similarity function for the given source model name.
   *
   * @param sourceModelName The source model name
   * @return The similarity function
   */
  public static SimilarityFunction getSimilarityFunction(String sourceModelName) {
    return getSimilarityFunction(fromString(sourceModelName));
  }

  /**
   * Converts a string representation of a source model name to its corresponding {@link
   * SourceModel} enum.
   *
   * <p>If the provided name is {@code null}, returns {@link SourceModel#UNDEFINED}, used for
   * non-vector collections. If the name is an empty string, returns the default {@link
   * SourceModel#OTHER}, indicating the collection was created before source models were supported.
   * Throws a {@link JsonApiException} if the name is unrecognized.
   *
   * @param name
   * @return
   * @throws JsonApiException
   */
  public static SourceModel fromString(String name) throws JsonApiException {
    if (name == null) return UNDEFINED;
    // The string may be empty if the collection was created before supporting source models
    if (name.isEmpty()) return OTHER;
    SourceModel model = SOURCE_MODEL_NAME_MAP.get(name);
    if (model == null) {
      String acceptedModels =
          SOURCE_MODEL_NAME_MAP.keySet().stream()
              .filter(key -> !key.equals(UNDEFINED.getName()))
              .collect(Collectors.joining(", "));
      throw ErrorCodeV1.VECTOR_SEARCH_UNRECOGNIZED_SOURCE_MODEL_NAME.toApiException(
          "Received: '%s'; Accepted: %s", name, acceptedModels);
    }
    return model;
  }
}
