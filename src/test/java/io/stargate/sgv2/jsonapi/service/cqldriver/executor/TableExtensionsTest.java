package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TableExtensionsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void schemaTypeAndVersionAlwaysPresent() {
    var props = TableExtensions.createCustomProperties(Map.of(), Map.of(), MAPPER);

    assertThat(props)
        .containsEntry(
            SchemaConstants.MetadataFieldsNames.SCHEMA_TYPE,
            SchemaConstants.MetadataFieldsValues.SCHEMA_TYPE_TABLE_VALUE)
        .containsEntry(
            SchemaConstants.MetadataFieldsNames.SCHEMA_VERSION,
            SchemaConstants.MetadataFieldsValues.SCHEMA_VERSION_VERSION)
        .doesNotContainKey(SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG)
        .doesNotContainKey(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES);
  }

  @Test
  void writesIndexProfilesWhenPresent() {
    var profiles =
        Map.of(
            "my_idx",
            new VectorIndexProfileDefinition(
                "small-high-recall", Map.of("maximum_node_connections", "32")));

    var props = TableExtensions.createCustomProperties(Map.of(), profiles, MAPPER);

    assertThat(props).containsKey(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES);
    // the written value round-trips back to the same profiles
    assertThat(
            VectorIndexProfileDefinition.fromJson(
                props.get(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES), MAPPER))
        .isEqualTo(profiles);
  }

  @Test
  void preservesVectorizeAndProfilesTogether() {
    // both keys written in one payload, so an extension rewrite carrying both does not lose either
    var vectorDefs =
        Map.of(
            CqlIdentifier.fromInternal("v"),
            new VectorizeDefinition("openai", "text-embedding-3-small", null, null));
    var profiles =
        Map.of(
            "v_idx",
            new VectorIndexProfileDefinition(
                "big-low-latency", Map.of("maximum_node_connections", "16")));

    var props = TableExtensions.createCustomProperties(vectorDefs, profiles, MAPPER);

    assertThat(props)
        .containsKey(SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG)
        .containsKey(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES);
  }

  @Test
  void twoArgOverloadOmitsProfiles() {
    var props = TableExtensions.createCustomProperties(Map.of(), MAPPER);

    assertThat(props).doesNotContainKey(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES);
  }

  @Nested
  class RemoveIndexProfile {

    private static final CqlIdentifier MY_IDX = CqlIdentifier.fromInternal("my_idx");

    @Test
    void emptyWhenNoTableOwnsTheIndex() {
      // the only table in the keyspace carries a different index
      var keyspace =
          keyspace(
              table(
                  "other_table",
                  Set.of(CqlIdentifier.fromInternal("some_other_idx")),
                  Map.of(
                      SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES,
                      profilesJson("some_other_idx"))));

      assertThat(TableExtensions.removeIndexProfile(keyspace, MY_IDX, MAPPER)).isEmpty();
    }

    @Test
    void emptyWhenOwningTableHasNoProfileForTheIndex() {
      // the owning table has a profiles blob, but not for the index being dropped
      var keyspace =
          keyspace(
              table(
                  "my_table",
                  Set.of(MY_IDX),
                  Map.of(
                      SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES,
                      profilesJson("unrelated_idx"))));

      assertThat(TableExtensions.removeIndexProfile(keyspace, MY_IDX, MAPPER)).isEmpty();
    }

    @Test
    void removesProfileAndPreservesOtherProfilesAndVectorize() {
      var keyspace =
          keyspace(
              table(
                  "my_table",
                  Set.of(MY_IDX),
                  Map.of(
                      SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES,
                      profilesJson("my_idx", "kept_idx"),
                      SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG,
                      "{\"v\":{\"provider\":\"openai\",\"modelName\":\"text-embedding-3-small\"}}")));

      var removal = TableExtensions.removeIndexProfile(keyspace, MY_IDX, MAPPER);

      assertThat(removal).isPresent();
      assertThat(removal.get().tableName()).isEqualTo(CqlIdentifier.fromInternal("my_table"));

      var customProperties = removal.get().customProperties();
      // schema type/version always written
      assertThat(customProperties)
          .containsKey(SchemaConstants.MetadataFieldsNames.SCHEMA_TYPE)
          .containsKey(SchemaConstants.MetadataFieldsNames.SCHEMA_VERSION)
          // vectorize config is read back and preserved
          .containsKey(SchemaConstants.MetadataFieldsNames.VECTORIZE_CONFIG);

      // the dropped index's profile is gone, the other index's profile is kept
      var profiles =
          VectorIndexProfileDefinition.fromJson(
              customProperties.get(SchemaConstants.MetadataFieldsNames.VECTOR_INDEX_PROFILES),
              MAPPER);
      assertThat(profiles).containsOnlyKeys("kept_idx");
    }

    /** Builds a {@code {index: {profile, options}}} blob for the given index keys. */
    private static String profilesJson(String... indexKeys) {
      var profiles = new HashMap<String, VectorIndexProfileDefinition>();
      for (var key : indexKeys) {
        profiles.put(key, new VectorIndexProfileDefinition("small-high-recall", Map.of()));
      }
      try {
        return MAPPER.writeValueAsString(profiles);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static KeyspaceMetadata keyspace(TableMetadata... tables) {
      var keyspaceMetadata = mock(KeyspaceMetadata.class);
      Map<CqlIdentifier, TableMetadata> tableMap = new HashMap<>();
      for (var table : tables) {
        tableMap.put(table.getName(), table);
      }
      when(keyspaceMetadata.getTables()).thenReturn(tableMap);
      return keyspaceMetadata;
    }

    private static TableMetadata table(
        String name, Set<CqlIdentifier> indexNames, Map<String, String> extensions) {
      var tableMetadata = mock(TableMetadata.class);
      when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(name));

      Map<CqlIdentifier, IndexMetadata> indexes = new HashMap<>();
      for (var indexName : indexNames) {
        indexes.put(indexName, mock(IndexMetadata.class));
      }
      when(tableMetadata.getIndexes()).thenReturn(indexes);

      Map<String, ByteBuffer> extensionBuffers = new HashMap<>();
      extensions.forEach(
          (key, value) ->
              extensionBuffers.put(key, ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))));
      Map<CqlIdentifier, Object> options = new HashMap<>();
      options.put(TableExtensions.TABLE_OPTIONS_EXTENSION_KEY, extensionBuffers);
      when(tableMetadata.getOptions()).thenReturn(options);

      return tableMetadata;
    }
  }
}
