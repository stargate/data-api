package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;

/**
 * Configuration vector column with the extra info we need for vectors
 *
 * @param fieldName Is still a string because this is also used by collections
 * @param vectorSize
 * @param similarityFunction
 * @param sourceModel
 * @param vectorizeDefinition
 */
public record VectorColumnDefinition(
    String fieldName,
    int vectorSize,
    SimilarityFunction similarityFunction,
    EmbeddingSourceModel sourceModel,
    VectorizeDefinition vectorizeDefinition) {

  /**
   * Convert from JSON.
   *
   * <p>NOTE: TODO: not clear what compatibility this has to has, e.g. same as the public JSON SPEC
   * ?
   */
  public static VectorColumnDefinition fromJson(JsonNode jsonNode, ObjectMapper objectMapper) {

    // dimension, similarityFunction, must exist
    // TODO: What if they do not exist ? What is enforcing this ?

    int dimension = jsonNode.get(VectorConstants.VectorColumn.DIMENSION).asInt();
    // aaron - 11 nov 2024, this code had no null check on the function name, and the
    // SimilarityFunction
    // turns nulls into UNDEFINED, so unclear what happens if the value was null here
    var functionName = jsonNode.get(VectorConstants.VectorColumn.METRIC).asText();
    var similarityFunction =
        SimilarityFunction.fromApiName(functionName)
            .orElseThrow(() -> SimilarityFunction.getUnknownFunctionException(functionName));
    // sourceModel doesn't exist if the collection was created before supporting sourceModel; if
    // missing, it will be an empty string and sourceModel becomes OTHER.
    // TODO: CHANGE THIS TO BE MORE EXPLICIT AND NOT RELY ON EMPTY STRING
    var sourceModel =
        EmbeddingSourceModel.fromName(
                jsonNode.path(VectorConstants.VectorColumn.SOURCE_MODEL).asText())
            .orElseThrow(
                () ->
                    EmbeddingSourceModel.getUnknownSourceModelException(
                        jsonNode.path(VectorConstants.VectorColumn.SOURCE_MODEL).asText()));

    return fromJson(
        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
        dimension,
        similarityFunction,
        sourceModel,
        jsonNode,
        objectMapper);
  }

  /** Convert a vector jsonNode from table extension to vectorConfig, used for tables */
  public static VectorColumnDefinition fromJson(
      String fieldName,
      int dimension,
      SimilarityFunction similarityFunction,
      EmbeddingSourceModel sourceModel,
      JsonNode jsonNode,
      ObjectMapper objectMapper) {

    JsonNode vectorizeServiceNode = jsonNode.get(VectorConstants.VectorColumn.SERVICE);
    var vectorizeDefinition =
        vectorizeServiceNode == null
            ? null
            : VectorizeDefinition.fromJson(vectorizeServiceNode, objectMapper);

    return new VectorColumnDefinition(
        fieldName, dimension, similarityFunction, sourceModel, vectorizeDefinition);
  }
}
