package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

/**
 * incorporates vectorizeConfig into vectorConfig
 *
 * @param vectorEnabled
 * @param vectorSize
 * @param similarityFunction
 * @param vectorizeConfig
 */
public record VectorConfig(
    boolean vectorEnabled,
    int vectorSize,
    CollectionSchemaObject.SimilarityFunction similarityFunction,
    VectorizeConfig vectorizeConfig) {

  // TODO: this is an immutable record, this can be singleton
  // TODO: Remove the use of NULL for the objects like vectorizeConfig
  public static VectorConfig notEnabledVectorConfig() {
    return new VectorConfig(false, -1, null, null);
  }

  // convert a vector jsonNode from table comment to vectorConfig
  public static VectorConfig fromJson(JsonNode jsonNode, ObjectMapper objectMapper) {
    // dimension, similarityFunction, must exist
    int dimension = jsonNode.get("dimension").asInt();
    CollectionSchemaObject.SimilarityFunction similarityFunction =
        CollectionSchemaObject.SimilarityFunction.fromString(jsonNode.get("metric").asText());

    VectorizeConfig vectorizeConfig = null;
    // construct vectorizeConfig
    JsonNode vectorizeServiceNode = jsonNode.get("service");
    if (vectorizeServiceNode != null) {
      // provider, modelName, must exist
      String provider = vectorizeServiceNode.get("provider").asText();
      String modelName = vectorizeServiceNode.get("modelName").asText();
      // construct VectorizeConfig.authentication, can be null
      JsonNode vectorizeServiceAuthenticationNode = vectorizeServiceNode.get("authentication");
      Map<String, String> vectorizeServiceAuthentication =
          vectorizeServiceAuthenticationNode == null
              ? null
              : objectMapper.convertValue(vectorizeServiceAuthenticationNode, Map.class);
      // construct VectorizeConfig.parameters, can be null
      JsonNode vectorizeServiceParameterNode = vectorizeServiceNode.get("parameters");
      Map<String, Object> vectorizeServiceParameter =
          vectorizeServiceParameterNode == null
              ? null
              : objectMapper.convertValue(vectorizeServiceParameterNode, Map.class);
      vectorizeConfig =
          new VectorizeConfig(
              provider, modelName, vectorizeServiceAuthentication, vectorizeServiceParameter);
    }

    return new VectorConfig(true, dimension, similarityFunction, vectorizeConfig);
  }

  public record VectorizeConfig(
      String provider,
      String modelName,
      Map<String, String> authentication,
      Map<String, Object> parameters) {}
}
