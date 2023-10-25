package io.stargate.sgv2.jsonapi.service.bridge.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.VECTORIZECONFIG_CHECK_FAIL;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Optional;

/**
 * Refactored as seperate class that represent a collection property.
 *
 * @param collectionName
 * @param vectorEnabled
 * @param vectorSize
 * @param similarityFunction
 * @param vectorizeServiceName
 * @param modelName
 */
public record CollectionSettings(
    String collectionName,
    boolean vectorEnabled,
    int vectorSize,
    SimilarityFunction similarityFunction,
    String vectorizeServiceName,
    String modelName) {

  /**
   * The similarity function used for the vector index. This is only applicable if the vector index
   * is enabled.
   */
  public enum SimilarityFunction {
    COSINE,
    EUCLIDEAN,
    DOT_PRODUCT,
    UNDEFINED;

    public static SimilarityFunction fromString(String similarityFunction) {
      if (similarityFunction == null) return UNDEFINED;
      return switch (similarityFunction.toLowerCase()) {
        case "cosine" -> COSINE;
        case "euclidean" -> EUCLIDEAN;
        case "dot_product" -> DOT_PRODUCT;
        default -> throw new JsonApiException(
            ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME,
            ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME.getMessage() + similarityFunction);
      };
    }
  }

  // TODO:need debug
  public static CollectionSettings getCollectionSettings(
      TableMetadata table, ObjectMapper objectMapper) {
    String collectionName = table.getName().asCql(true);
    final Optional<ColumnMetadata> vectorColumn =
        table.getColumn(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME);

    boolean vectorEnabled = vectorColumn.isPresent();
    if (vectorEnabled) {
      final int vectorSize =
          ((VectorType) vectorColumn.get().getType()).getDimensions();
      final IndexMetadata vectorIndex =
          table
              .getIndexes()
              .get(CqlIdentifier.fromCql(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME));
      CollectionSettings.SimilarityFunction function = CollectionSettings.SimilarityFunction.COSINE;

      if (vectorIndex != null) {
        final String functionName =
            vectorIndex.getOptions().get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME);
        if (functionName != null)
          function = CollectionSettings.SimilarityFunction.fromString(functionName);
      }
      final String comment = (String) table.getOptions().get("comment");
      if (comment != null && !comment.isBlank()) {
        return createCollectionSettingsFromJson(
            collectionName, vectorEnabled, vectorSize, function, comment, objectMapper);
      } else {
        return new CollectionSettings(
            collectionName, vectorEnabled, vectorSize, function, null, null);
      }
    } else {
      return new CollectionSettings(
          collectionName,
          vectorEnabled,
          0,
          CollectionSettings.SimilarityFunction.UNDEFINED,
          null,
          null);
    }
  }

  public static CollectionSettings getCollectionSettings(
      Schema.CqlTable table, ObjectMapper objectMapper) {
    String collectionName = table.getName();
    final Optional<QueryOuterClass.ColumnSpec> first =
        table.getColumnsList().stream()
            .filter(
                c -> c.getName().equals(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME))
            .findFirst();
    boolean vectorEnabled = first.isPresent();
    if (vectorEnabled) {
      final int vectorSize = first.get().getType().getVector().getSize();
      final Optional<Schema.CqlIndex> vectorIndex =
          table.getIndexesList().stream()
              .filter(
                  i ->
                      i.getColumnName()
                          .equals(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME))
              .findFirst();
      CollectionSettings.SimilarityFunction function = CollectionSettings.SimilarityFunction.COSINE;
      if (vectorIndex.isPresent()) {
        final String functionName =
            vectorIndex
                .get()
                .getOptionsMap()
                .get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME);
        if (functionName != null)
          function = CollectionSettings.SimilarityFunction.fromString(functionName);
      }
      final String comment = table.getOptionsOrDefault("comment", null);
      if (comment != null && !comment.isBlank()) {
        return createCollectionSettingsFromJson(
            collectionName, vectorEnabled, vectorSize, function, comment, objectMapper);
      } else {
        return new CollectionSettings(
            collectionName, vectorEnabled, vectorSize, function, null, null);
      }
    } else {
      return new CollectionSettings(
          collectionName,
          vectorEnabled,
          0,
          CollectionSettings.SimilarityFunction.UNDEFINED,
          null,
          null);
    }
  }

  public static CollectionSettings getCollectionSettings(
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      String vectorize,
      ObjectMapper objectMapper) {
    // parse vectorize to get vectorizeServiceName and modelName
    if (vectorize != null && !vectorize.isBlank()) {
      return createCollectionSettingsFromJson(
          collectionName, vectorEnabled, vectorSize, similarityFunction, vectorize, objectMapper);
    } else {
      return new CollectionSettings(
          collectionName, vectorEnabled, vectorSize, similarityFunction, null, null);
    }
  }

  private static CollectionSettings createCollectionSettingsFromJson(
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      String vectorize,
      ObjectMapper objectMapper) {
    try {
      JsonNode vectorizeConfig = objectMapper.readTree(vectorize);
      String vectorizeServiceName = vectorizeConfig.path("service").textValue();
      JsonNode optionsNode = vectorizeConfig.path("options");
      String modelName = optionsNode.path("modelName").textValue();
      if (vectorizeServiceName != null
          && !vectorizeServiceName.isEmpty()
          && modelName != null
          && !modelName.isEmpty()) {
        return new CollectionSettings(
            collectionName, vectorEnabled, vectorSize, function, vectorizeServiceName, modelName);
      } else {
        // This should never happen, VectorizeConfig check null, unless it fails
        throw new JsonApiException(
            VECTORIZECONFIG_CHECK_FAIL,
            "%s, please check 'vectorize' configuration."
                .formatted(VECTORIZECONFIG_CHECK_FAIL.getMessage()));
      }
    } catch (JsonProcessingException e) {
      // This should never happen, already check if vectorize is a valid JSON
      throw new RuntimeException("Invalid json string, please check 'vectorize' configuration.", e);
    }
  }
}
