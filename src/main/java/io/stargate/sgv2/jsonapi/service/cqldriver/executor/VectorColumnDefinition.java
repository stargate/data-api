package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;

/**
 * Configuration vector column with the extra info we need for vectors
 *
 * @param fieldName Is still a string because this is also used by collections
 * @param vectorSize
 * @param similarityFunction
 * @param vectorizeDefinition
 */
public record VectorColumnDefinition(
    String fieldName,
    int vectorSize,
    SimilarityFunction similarityFunction,
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
    var similarityFunction =
        SimilarityFunction.fromString(jsonNode.get(VectorConstants.VectorColumn.METRIC).asText());

    return fromJson(
        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
        dimension,
        similarityFunction,
        jsonNode,
        objectMapper);
  }

  /** Convert a vector jsonNode from table extension to vectorConfig, used for tables */
  public static VectorColumnDefinition fromJson(
      String fieldName,
      int dimension,
      SimilarityFunction similarityFunction,
      JsonNode jsonNode,
      ObjectMapper objectMapper) {

    JsonNode vectorizeServiceNode = jsonNode.get(VectorConstants.VectorColumn.SERVICE);
    var vectorizeDefinition =
        vectorizeServiceNode == null
            ? null
            : VectorizeDefinition.fromJson(vectorizeServiceNode, objectMapper);

    return new VectorColumnDefinition(
        fieldName, dimension, similarityFunction, vectorizeDefinition);
  }
}
