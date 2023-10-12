package io.stargate.sgv2.jsonapi.service.bridge.executor;

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
        try {
          JsonNode vectorizeConfig = objectMapper.readTree(comment);
          String vectorizeServiceName = vectorizeConfig.get("service").textValue();
          final JsonNode optionsNode = vectorizeConfig.get("options");
          String modelName = optionsNode.get("modelName").textValue();
          return new CollectionSettings(
              collectionName, vectorEnabled, vectorSize, function, vectorizeServiceName, modelName);
        } catch (JsonProcessingException e) {
          // This should never happen
          throw new RuntimeException(e);
        }
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
      try {
        JsonNode vectorizeConfig = objectMapper.readTree(vectorize);
        String vectorizeServiceName_ = vectorizeConfig.get("service").textValue();
        final JsonNode optionsNode = vectorizeConfig.get("options");
        String modelName_ = optionsNode.get("modelName").textValue();
        return new CollectionSettings(
            collectionName,
            vectorEnabled,
            vectorSize,
            similarityFunction,
            vectorizeServiceName_,
            modelName_);
      } catch (JsonProcessingException e) {
        // This should never happen
        throw new RuntimeException(e);
      }
    } else {
      return new CollectionSettings(
          collectionName, vectorEnabled, vectorSize, similarityFunction, null, null);
    }
  }
}
