package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  /**
   * Create custom properties for table metadata, This needs to add schema and table always since
   * the command may be altering CQL created tables
   */
  public static Map<String, String> createCustomProperties(
      Map<CqlIdentifier, VectorizeDefinition> vectorDefs, ObjectMapper objectMapper) {
    Objects.requireNonNull(vectorDefs, "vectorDefs must not be null");
    Objects.requireNonNull(objectMapper, "objectMapper must not be null");

    Map<String, String> customProperties = new HashMap<>();
    customProperties.put(
        SchemaConstants.MetadataFieldsNames.SCHEMA_TYPE,
        SchemaConstants.MetadataFieldsValues.SCHEMA_TYPE_TABLE_VALUE);
    // Versioning for schema json. This needs can be adapted in future as needed
    customProperties.put(
        SchemaConstants.MetadataFieldsNames.SCHEMA_VERSION,
        SchemaConstants.MetadataFieldsValues.SCHEMA_VERSION_VERSION);

    // because the extensions are always fully replaced, we do not need to write the key if there
    // are none
    // the full map will be replaced, replacing any existing extensions
    if (!vectorDefs.isEmpty()) {
      // convert to strings for serialisation
      Map<String, VectorizeDefinition> stringKeysDefs =
          vectorDefs.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> cqlIdentifierToJsonKey(entry.getKey()), Map.Entry::getValue));

      try {
        customProperties.put(
            SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG,
            objectMapper.writeValueAsString(stringKeysDefs));
      } catch (JsonProcessingException e) {
        // this should never happen
        throw new RuntimeException(e);
      }
    }
    return customProperties;
  }
}
