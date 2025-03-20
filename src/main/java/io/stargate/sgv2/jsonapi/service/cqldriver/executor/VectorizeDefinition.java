package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.VectorColumnDesc;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How to vectorize strings for a column
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
    Map<String, Object> parameters)
    implements Recordable {

  private static Logger LOGGER = LoggerFactory.getLogger(VectorizeDefinition.class);

  /**
   * Used to build from the JSON API description
   *
   * <p>NOTE: Will throw if {@link VectorizeConfigValidator#validateService(VectorizeConfig,
   * Integer)} throws.
   */
  public static VectorizeDefinition from(
      VectorColumnDesc vectorColumnDesc,
      int dimension,
      VectorizeConfigValidator validateVectorize) {

    return from(vectorColumnDesc.getVectorizeConfig(), dimension, validateVectorize);
  }

  public static VectorizeDefinition from(
      VectorizeConfig vectorizeDesc, int dimension, VectorizeConfigValidator validateVectorize) {

    // aaron - I think this is correct, it's a vector column but the vectorize config is null
    if (vectorizeDesc == null) {
      return null;
    }

    validateVectorize.validateService(vectorizeDesc, dimension);

    return new VectorizeDefinition(
        vectorizeDesc.provider(),
        vectorizeDesc.modelName(),
        vectorizeDesc.authentication(),
        vectorizeDesc.parameters());
  }

  static VectorizeDefinition fromJson(JsonNode jsonNode, ObjectMapper objectMapper) {

    // provider, modelName, must exist
    // TODO: WHAT HAPPENS IF THEY DONT ? JSON props on VectorizeConfig say model is not required
    String provider = jsonNode.get(VectorConstants.Vectorize.PROVIDER).asText();
    String modelName = jsonNode.get(VectorConstants.Vectorize.MODEL_NAME).asText();

    // construct VectorizeDefinition.authentication, can be null
    JsonNode authNode = jsonNode.get(VectorConstants.Vectorize.AUTHENTICATION);
    // TODO: remove unchecked assignment
    Map<String, String> authMap =
        authNode == null ? null : objectMapper.convertValue(authNode, Map.class);

    // construct VectorizeDefinition.parameters, can be null
    JsonNode paramsNode = jsonNode.get(VectorConstants.Vectorize.PARAMETERS);
    // TODO: remove unchecked assignment
    Map<String, Object> paramsMap =
        paramsNode == null ? null : objectMapper.convertValue(paramsNode, Map.class);

    return new VectorizeDefinition(provider, modelName, authMap, paramsMap);
  }

  public static Map<String, VectorizeDefinition> from(
      TableMetadata tableMetadata, ObjectMapper objectMapper) {

    Map<String, String> extensions = TableExtensions.getExtensions(tableMetadata);

    Map<String, VectorizeDefinition> defs = new HashMap<>();
    String vectorizeJson = extensions.get(SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG);
    if (vectorizeJson == null) {
      return defs;
    }

    try {
      JsonNode vectorizeByColumns = objectMapper.readTree(vectorizeJson);
      for (Map.Entry<String, JsonNode> entry : vectorizeByColumns.properties()) {
        // key is the column name, value is the vectorize definition

        VectorizeDefinition vectorizeDef = null;
        try {
          vectorizeDef = objectMapper.treeToValue(entry.getValue(), VectorizeDefinition.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
          LOGGER.error(
              "Error parsing vectorize JSON configuration for table: %s.%s, column: %s , json:%s"
                  .formatted(
                      tableMetadata.getKeyspace(),
                      tableMetadata.getName(),
                      entry.getKey(),
                      entry.getValue().toString()),
              e);

          // TODO: Update this error so it says the keyspace and table name !
          throw SchemaException.Code.INVALID_VECTORIZE_CONFIGURATION.get(
              Map.of("field", entry.getKey()));
        }
        defs.put(entry.getKey(), vectorizeDef);
      }
    } catch (JsonProcessingException e) {
      LOGGER.error(
          "Error parsing vectorize JSON configuration for table: %s.%s, json: %s"
              .formatted(tableMetadata.getKeyspace(), tableMetadata.getName(), vectorizeJson),
          e);
      // TODO: THIS ERROR NEEDS WORK !!!! Update this error so it says the keyspace and table name !
      throw SchemaException.Code.INVALID_CONFIGURATION.get();
    }
    return defs;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("provider", provider)
        .append("modelName", modelName)
        .append("authentication", authentication)
        .append("parameters", parameters);
  }

  public VectorizeConfig toVectorizeConfig() {
    return new VectorizeConfig(provider, modelName, authentication, parameters);
  }
}
