package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Definition of vector config for a collection or table */
public class VectorConfig {
  private final List<ColumnVectorDefinition> columnVectorDefinitions;
  private final boolean vectorEnabled;

  /*
   * @param columnVectorDefinitions - List of columnVectorDefinitions each with respect to a
   *     column/field
   */
  private VectorConfig(
      List<ColumnVectorDefinition> columnVectorDefinitions, boolean vectorEnabled) {
    this.columnVectorDefinitions = columnVectorDefinitions;
    this.vectorEnabled = vectorEnabled;
  }

  public static VectorConfig fromColumnDefinitions(
      List<ColumnVectorDefinition> columnVectorDefinitions) {
    if (columnVectorDefinitions == null || columnVectorDefinitions.isEmpty()) {
      return NOT_ENABLED_CONFIG;
    }
    return new VectorConfig(Collections.unmodifiableList(columnVectorDefinitions), true);
  }

  public static final VectorConfig NOT_ENABLED_CONFIG = new VectorConfig(null, false);

  public boolean vectorEnabled() {
    return vectorEnabled;
  }

  public List<ColumnVectorDefinition> columnVectorDefinitions() {
    return columnVectorDefinitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorConfig that = (VectorConfig) o;
    return vectorEnabled == that.vectorEnabled
        && Objects.equals(columnVectorDefinitions, that.columnVectorDefinitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnVectorDefinitions, vectorEnabled);
  }

  /**
   * Configuration for a column, In case of collection this will be of size one
   *
   * @param fieldName
   * @param vectorSize
   * @param similarityFunction
   * @param vectorizeDefinition
   */
  public record ColumnVectorDefinition(
      String fieldName,
      int vectorSize,
      SimilarityFunction similarityFunction,
      VectorizeDefinition vectorizeDefinition) {

    // convert a vector jsonNode from comment option to vectorConfig, used for collection
    public static ColumnVectorDefinition fromJson(JsonNode jsonNode, ObjectMapper objectMapper) {
      // dimension, similarityFunction, must exist
      int dimension = jsonNode.get("dimension").asInt();
      SimilarityFunction similarityFunction =
          SimilarityFunction.fromString(jsonNode.get("metric").asText());

      return fromJson(
          DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
          dimension,
          similarityFunction,
          jsonNode,
          objectMapper);
    }

    // convert a vector jsonNode from table extension to vectorConfig, used for tables
    public static ColumnVectorDefinition fromJson(
        String fieldName,
        int dimension,
        SimilarityFunction similarityFunction,
        JsonNode jsonNode,
        ObjectMapper objectMapper) {
      VectorizeDefinition vectorizeDefinition = null;
      // construct vectorizeDefinition
      JsonNode vectorizeServiceNode = jsonNode.get("service");
      if (vectorizeServiceNode != null) {
        vectorizeDefinition = VectorizeDefinition.fromJson(vectorizeServiceNode, objectMapper);
      }
      return new ColumnVectorDefinition(
          fieldName, dimension, similarityFunction, vectorizeDefinition);
    }

    /**
     * Represent the vectorize configuration defined for a column
     *
     * @param provider
     * @param modelName
     * @param authentication
     * @param parameters
     */
    public record VectorizeDefinition(
        String provider,
        String modelName,
        Map<String, String> authentication,
        Map<String, Object> parameters) {

      protected static VectorizeDefinition fromJson(
          JsonNode vectorizeServiceNode, ObjectMapper objectMapper) {
        // provider, modelName, must exist
        String provider = vectorizeServiceNode.get("provider").asText();
        String modelName = vectorizeServiceNode.get("modelName").asText();
        // construct VectorizeDefinition.authentication, can be null
        JsonNode vectorizeServiceAuthenticationNode = vectorizeServiceNode.get("authentication");
        Map<String, String> vectorizeServiceAuthentication =
            vectorizeServiceAuthenticationNode == null
                ? null
                : objectMapper.convertValue(vectorizeServiceAuthenticationNode, Map.class);
        // construct VectorizeDefinition.parameters, can be null
        JsonNode vectorizeServiceParameterNode = vectorizeServiceNode.get("parameters");
        Map<String, Object> vectorizeServiceParameter =
            vectorizeServiceParameterNode == null
                ? null
                : objectMapper.convertValue(vectorizeServiceParameterNode, Map.class);
        return new VectorizeDefinition(
            provider, modelName, vectorizeServiceAuthentication, vectorizeServiceParameter);
      }

      public VectorizeConfig toVectorizeConfig() {
        return new VectorizeConfig(provider, modelName, authentication, parameters);
      }
    }
  }
}
