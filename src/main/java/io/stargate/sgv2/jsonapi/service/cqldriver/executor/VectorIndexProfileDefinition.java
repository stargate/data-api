package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The profile a vector index was created with: the profile name plus the SAI options it expanded
 * to. Stored per index name in the table extensions (key {@link
 * SchemaConstants.MetadataFieldsNames#VECTOR_INDEX_PROFILES}) to keep the profile name. The options
 * snapshot stays valid even if the profile definition changes later.
 */
public record VectorIndexProfileDefinition(String profile, Map<String, String> options) {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexProfileDefinition.class);

  /** Reads the stored profiles, keyed by index name, from the table extensions. */
  public static Map<String, VectorIndexProfileDefinition> from(
      TableMetadata tableMetadata, ObjectMapper objectMapper) {
    var extensions = TableExtensions.getExtensions(tableMetadata);
    return fromJson(
        extensions.get(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES), objectMapper);
  }

  /**
   * Parses the {@code index name -> profile} JSON from the extensions. Returns a mutable map so
   * callers can merge changes before writing it back. Profiles are advisory metadata, so a bad blob
   * is logged and skipped, not failed.
   */
  static Map<String, VectorIndexProfileDefinition> fromJson(
      String json, ObjectMapper objectMapper) {
    Map<String, VectorIndexProfileDefinition> defs = new HashMap<>();
    if (json == null || json.isBlank()) {
      return defs;
    }
    try {
      JsonNode byIndex = objectMapper.readTree(json);
      for (Map.Entry<String, JsonNode> entry : byIndex.properties()) {
        defs.put(
            entry.getKey(),
            objectMapper.treeToValue(entry.getValue(), VectorIndexProfileDefinition.class));
      }
    } catch (JsonProcessingException | IllegalArgumentException e) {
      LOGGER.error("Error parsing vector index profiles, json: {}", json, e);
      defs.clear();
    }
    return defs;
  }

  /**
   * Records the profile for {@code indexKey} in {@code profiles}, or removes any stale entry when
   * {@code def} is null (no profile was used). Returns true if the map changed, letting the caller
   * skip an unneeded extension write.
   */
  public static boolean putOrRemove(
      Map<String, VectorIndexProfileDefinition> profiles,
      String indexKey,
      VectorIndexProfileDefinition def) {
    if (def == null) {
      return profiles.remove(indexKey) != null;
    }
    return !def.equals(profiles.put(indexKey, def));
  }
}
