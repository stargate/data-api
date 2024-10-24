package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;

public class TableMetadataUtils {
  private static Logger logger = org.slf4j.LoggerFactory.getLogger(TableMetadataUtils.class);

  /** Get extension Map from table metadata */
  public static Map<String, String> getExtensions(TableMetadata tableMetadata) {
    Map<String, ByteBuffer> extensionsBuffer =
        (Map<String, ByteBuffer>)
            tableMetadata.getOptions().get(CqlIdentifier.fromInternal("extensions"));

    Map<String, String> extensions = new HashMap<>();

    if (extensionsBuffer != null) {
      for (Map.Entry<String, ByteBuffer> entry : extensionsBuffer.entrySet()) {
        extensions.put(
            entry.getKey(),
            new String(ByteUtils.getArray(entry.getValue().duplicate()), StandardCharsets.UTF_8));
      }
    }
    return extensions;
  }

  /** Deserialize vectorize config from extensions */
  public static Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition>
      getVectorizeMap(Map<String, String> extensions, ObjectMapper objectMapper) {
    Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition> vectorizeConfigMap =
        new HashMap<>();
    String vectorizeJson = extensions.get(SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG);

    if (vectorizeJson != null) {
      try {
        JsonNode vectorizeByColumns = objectMapper.readTree(vectorizeJson);
        Iterator<Map.Entry<String, JsonNode>> it = vectorizeByColumns.fields();
        while (it.hasNext()) {
          Map.Entry<String, JsonNode> entry = it.next();
          try {
            var vectorizeConfig =
                objectMapper.treeToValue(
                    entry.getValue(),
                    VectorConfig.ColumnVectorDefinition.VectorizeDefinition.class);
            vectorizeConfigMap.put(entry.getKey(), vectorizeConfig);
          } catch (JsonProcessingException | IllegalArgumentException e) {
            logger.error("Unable to parse the config json", e);
            throw SchemaException.Code.INVALID_VECTORIZE_CONFIGURATION.get(
                Map.of("field", entry.getKey()));
          }
        }
      } catch (JsonProcessingException e) {
        logger.error("Unable to parse the config json", e);
        throw SchemaException.Code.INVALID_CONFIGURATION.get();
      }
    }
    return vectorizeConfigMap;
  }

  /**
   * Create custom properties for table metadata, This needs to add schema and table always since
   * the command may be altering CQL created tables
   */
  public static Map<String, String> createCustomProperties(
      Map<String, VectorConfig.ColumnVectorDefinition.VectorizeDefinition> vectorizeConfigMap,
      ObjectMapper objectMapper) {
    Map<String, String> customProperties = new HashMap<>();
    try {
      customProperties.put(
          SchemaConstants.MetadataFieldsNames.SCHEMA_TYPE,
          SchemaConstants.MetadataFieldsValues.SCHEMA_TYPE_TABLE_VALUE);
      // Versioning for schema json. This needs can be adapted in future as needed
      customProperties.put(
          SchemaConstants.MetadataFieldsNames.SCHEMA_VERSION,
          SchemaConstants.MetadataFieldsValues.SCHEMA_VERSION_VERSION);
      if (vectorizeConfigMap != null) {
        String vectorizeConfigToStore = objectMapper.writeValueAsString(vectorizeConfigMap);
        customProperties.put(
            SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG, vectorizeConfigToStore);
      }
    } catch (JsonProcessingException e) {
      // this should never happen
      throw new RuntimeException(e);
    }
    return customProperties;
  }
}
