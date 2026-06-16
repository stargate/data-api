package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.SchemaConstants;
import java.util.Map;
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
}
