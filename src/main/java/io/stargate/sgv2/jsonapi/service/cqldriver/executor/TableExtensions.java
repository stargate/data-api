package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public abstract class TableExtensions {

  private static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TableExtensions.class);

  /** The key in the table metadata options map where extensions are stored. */
  public static final CqlIdentifier TABLE_OPTIONS_EXTENSION_KEY =
      CqlIdentifier.fromInternal("extensions");

  /**
   * Reads the extensions from the {@link TableMetadata} and returns all the keys as a map .
   *
   * <p>Note: This may include keys that are not part of the Data API
   *
   * @param tableMetadata The table metadata to read the extensions from
   * @return Map of the extensions where each ByteBuffer extension is converted to a String.
   */
  public static Map<String, String> getExtensions(TableMetadata tableMetadata) {

    Map<String, ByteBuffer> extensionsBuffer = uncheckedExtensions(tableMetadata);
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

  // Add customProperties which has table properties for vectorize
  // Convert value to hex string using the ByteUtils.toHexString
  // This needs to use `createTable.withExtensions()` method in driver when PR
  // (https://github.com/apache/cassandra-java-driver/pull/1964) is released
  public static Map<String, String> toExtensions(Map<String, String> customProperties) {
    return customProperties.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> ByteUtils.toHexString(e.getValue().getBytes())));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, ByteBuffer> uncheckedExtensions(TableMetadata tableMetadata) {
    return (Map<String, ByteBuffer>) tableMetadata.getOptions().get(TABLE_OPTIONS_EXTENSION_KEY);
  }

  /** As {@link #createCustomProperties(Map, Map, ObjectMapper)} with no vector index profiles. */
  public static Map<String, String> createCustomProperties(
      Map<CqlIdentifier, VectorizeDefinition> vectorDefs, ObjectMapper objectMapper) {
    return createCustomProperties(vectorDefs, Map.of(), objectMapper);
  }

  /**
   * Builds the table extensions payload: schema type/version (always written, since the command may
   * be altering a CQL-created table) plus the vectorize config and vector index profiles.
   *
   * <p>Extensions are fully replaced on every write, so callers must pass every def and profile
   * they want to keep; anything omitted is dropped.
   */
  public static Map<String, String> createCustomProperties(
      Map<CqlIdentifier, VectorizeDefinition> vectorDefs,
      Map<String, VectorIndexProfileDefinition> indexProfiles,
      ObjectMapper objectMapper) {
    Objects.requireNonNull(vectorDefs, "vectorDefs must not be null");
    Objects.requireNonNull(indexProfiles, "indexProfiles must not be null");
    Objects.requireNonNull(objectMapper, "objectMapper must not be null");

    Map<String, String> customProperties = new HashMap<>();
    customProperties.put(
        SchemaConstants.MetadataFieldsNames.SCHEMA_TYPE,
        SchemaConstants.MetadataFieldsValues.SCHEMA_TYPE_TABLE_VALUE);
    // Versioning for schema json. This needs can be adapted in future as needed
    customProperties.put(
        SchemaConstants.MetadataFieldsNames.SCHEMA_VERSION,
        SchemaConstants.MetadataFieldsValues.SCHEMA_VERSION_VERSION);

    // Only write a key when it has content (the map is fully replaced anyway).
    if (!vectorDefs.isEmpty()) {
      // convert to strings for serialisation
      Map<String, VectorizeDefinition> stringKeysDefs =
          vectorDefs.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> cqlIdentifierToJsonKey(entry.getKey()), Map.Entry::getValue));

      customProperties.put(
          SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG,
          writeJson(stringKeysDefs, objectMapper));
    }
    if (!indexProfiles.isEmpty()) {
      customProperties.put(
          SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES,
          writeJson(indexProfiles, objectMapper));
    }
    return customProperties;
  }

  /**
   * Computes the extensions payload that drops {@code indexName}'s vector-index profile from the
   * table that owns it, keeping the {@link
   * SchemaConstants.MetadataFieldsNames#VECTOR_INDEX_PROFILES} extension in sync so a profile does
   * not outlive its index.
   *
   * <p>The owning table is the one in {@code keyspaceMetadata} whose indexes contain {@code
   * indexName}. Returns empty (so the caller can skip the DDL) when no table owns the index or the
   * owning table has no stored profile for it.
   *
   * <p>On rewrite the existing vectorize config and the other indexes' profiles are read back and
   * included so the full-replace write does not lose them, as on the create side (see {@link
   * #createCustomProperties(Map, Map, ObjectMapper)}).
   */
  public static Optional<IndexProfileRemoval> removeIndexProfile(
      KeyspaceMetadata keyspaceMetadata, CqlIdentifier indexName, ObjectMapper objectMapper) {
    Objects.requireNonNull(keyspaceMetadata, "keyspaceMetadata must not be null");
    Objects.requireNonNull(indexName, "indexName must not be null");
    Objects.requireNonNull(objectMapper, "objectMapper must not be null");

    var owningTable =
        keyspaceMetadata.getTables().values().stream()
            .filter(table -> table.getIndexes().containsKey(indexName))
            .findFirst();
    if (owningTable.isEmpty()) {
      return Optional.empty();
    }

    var tableMetadata = owningTable.get();
    var profiles = VectorIndexProfileDefinition.from(tableMetadata, objectMapper);
    // null def => remove; false return => no entry existed, so there is nothing to rewrite.
    if (!VectorIndexProfileDefinition.putOrRemove(
        profiles, cqlIdentifierToJsonKey(indexName), null)) {
      return Optional.empty();
    }

    // Read the vectorize config back so the full-replace write preserves it. Stored keys are the
    // columns' internal form, so rebuild the CqlIdentifier keys createCustomProperties expects.
    var vectorDefs =
        VectorizeDefinition.from(tableMetadata, objectMapper).entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> CqlIdentifier.fromInternal(entry.getKey()), Map.Entry::getValue));

    var customProperties = createCustomProperties(vectorDefs, profiles, objectMapper);
    return Optional.of(new IndexProfileRemoval(tableMetadata.getName(), customProperties));
  }

  /**
   * Result of {@link #removeIndexProfile}: the table to alter and the extensions payload to write
   * (dropped index's profile removed, everything else preserved).
   */
  public record IndexProfileRemoval(
      CqlIdentifier tableName, Map<String, String> customProperties) {}

  private static String writeJson(Object value, ObjectMapper objectMapper) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }
}
