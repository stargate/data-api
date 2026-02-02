package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The source model used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum EmbeddingSourceModel {
  ADA002(ApiConstants.ADA002, "ADA002", SimilarityFunction.DOT_PRODUCT),
  BERT(ApiConstants.BERT, "BERT", SimilarityFunction.DOT_PRODUCT),
  COHERE_V3(ApiConstants.COHERE_V3, "COHERE_V3", SimilarityFunction.DOT_PRODUCT),
  GECKO(ApiConstants.GECKO, "GECKO", SimilarityFunction.DOT_PRODUCT),
  NV_QA_4(ApiConstants.NV_QA_4, "NV_QA_4", SimilarityFunction.DOT_PRODUCT),
  OPENAI_V3_LARGE(ApiConstants.OPENAI_V3_LARGE, "OPENAI_V3_LARGE", SimilarityFunction.DOT_PRODUCT),
  OPENAI_V3_SMALL(ApiConstants.OPENAI_V3_SMALL, "OPENAI_V3_SMALL", SimilarityFunction.DOT_PRODUCT),
  OTHER(ApiConstants.OTHER, "OTHER", SimilarityFunction.COSINE);

  /** For use with API swagger docs */
  public interface ApiConstants {
    String ADA002 = "ada002";
    String BERT = "bert";
    String COHERE_V3 = "cohere-v3";
    String GECKO = "gecko";
    String NV_QA_4 = "nv-qa-4";
    String OPENAI_V3_LARGE = "openai-v3-large";
    String OPENAI_V3_SMALL = "openai-v3-small";
    String OTHER = "other";

    String ALL =
        ADA002
            + ", "
            + BERT
            + ", "
            + COHERE_V3
            + ", "
            + GECKO
            + ", "
            + NV_QA_4
            + ", "
            + OPENAI_V3_LARGE
            + ", "
            + OPENAI_V3_SMALL
            + ", "
            + OTHER;
  }

  // TODO: Add a comment why this is the default
  public static final EmbeddingSourceModel DEFAULT = OTHER;

  private final String apiName;
  private final String cqlName;
  private final SimilarityFunction similarityFunction;

  private static final Map<String, EmbeddingSourceModel> SOURCE_MODEL_BY_API_NAME =
      Arrays.stream(EmbeddingSourceModel.values())
          .collect(Collectors.toMap(model -> model.apiName().toLowerCase(), Function.identity()));

  /** Do not use directly, see {@link #safeFromCqlName(String)}. */
  private static final Map<String, EmbeddingSourceModel> SOURCE_MODEL_BY_CQL_NAME =
      Arrays.stream(EmbeddingSourceModel.values())
          .collect(Collectors.toMap(model -> model.cqlName().toLowerCase(), Function.identity()));

  EmbeddingSourceModel(String apiName, String cqlName, SimilarityFunction similarityFunction) {
    this.apiName = apiName;
    this.cqlName = cqlName;
    this.similarityFunction = similarityFunction;
  }

  /**
   * The name the API uses when talking to CQL about this model, see {@link
   * #safeFromCqlName(String)} for some of the nuances.
   *
   * @return the name of the source model
   */
  public String cqlName() {
    return cqlName;
  }

  /**
   * The name the API uses when talking to the user about this model.
   *
   * @return the name of the source model
   */
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

  /** Gets a list of all of API names of the supported source models. */
  public static List<String> allApiNames() {
    return Arrays.stream(EmbeddingSourceModel.values()).map(EmbeddingSourceModel::apiName).toList();
  }

  /**
   * Converts a string representation of a source model name from the CQL index to its corresponding
   * {@link EmbeddingSourceModel} enum if it exists.
   *
   * @param cqlName the name of the source model from the cql index, nullable.
   * @return Optional of the corresponding {@link EmbeddingSourceModel} or empty if the cqlName was
   *     null or is not known.
   */
  public static Optional<EmbeddingSourceModel> fromCqlName(String cqlName) {
    return cqlName == null ? Optional.empty() : Optional.ofNullable(safeFromCqlName(cqlName));
  }

  /**
   * Like {@link #fromCqlName(String)} but if the cqlName is null or blank it will return {@link
   * #DEFAULT} model.
   *
   * @param cqlName the name of the source model from the cql index, nullable.
   * @return Optional of the corresponding {@link EmbeddingSourceModel} or {@link #DEFAULT} if the
   *     cqlName was null or blank, or empty if the cqlName was not known.
   */
  public static Optional<EmbeddingSourceModel> fromCqlNameOrDefault(String cqlName) {
    return switch (cqlName) {
      case null -> Optional.of(DEFAULT);
      case String s when s.isBlank() -> Optional.of(DEFAULT);
      default -> fromCqlName(cqlName);
    };
  }

  /**
   * The database will allow an index to be created with either "-" or "_" in the name. it replaces
   * all "-" with "_" when mapping from the value on the index options, but will then leave the "-"
   * in the options , so we have to suport both CQL names. See IndexWriterConfig in C* code
   *
   * <p>Examples of options from `system_schema.indexes` of valid indexes in the DB:
   *
   * <pre>
   * {'class_name': 'StorageAttachedIndex', 'source_model': 'openai_v3_small', 'target': 'comment_vector'}
   * {'class_name': 'StorageAttachedIndex', 'source_model': 'openai-v3-small', 'target': 'my_vector'}
   * </pre>
   *
   * @param cqlName the name of the source model from the cql index.
   * @return the source model or null if not found
   */
  private static EmbeddingSourceModel safeFromCqlName(String cqlName) {
    var model = SOURCE_MODEL_BY_CQL_NAME.get(cqlName.toLowerCase());
    return model != null
        ? model
        : SOURCE_MODEL_BY_CQL_NAME.get(cqlName.toLowerCase().replace("-", "_"));
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
  public static APIException getUnknownSourceModelException(String apiName) {
    return SchemaException.Code.VECTOR_SEARCH_UNRECOGNIZED_SOURCE_MODEL_NAME.get(
        Map.of(
            "errorMessage",
            "Received: '%s'; Accepted: %s".formatted(apiName, String.join(", ", allApiNames()))));
  }
}
