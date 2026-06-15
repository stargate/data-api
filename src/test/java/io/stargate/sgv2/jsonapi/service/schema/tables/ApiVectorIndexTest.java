package io.stargate.sgv2.jsonapi.service.schema.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code indexingOptions} handling on {@link ApiVectorIndex}: how the public
 * value (profile name or raw options) is turned into the CQL index options map, and how it is
 * rendered back for the schema description. These are deterministic and do not need a database (the
 * end-to-end behaviour also depends on the backend allowing custom SAI parameters).
 */
class ApiVectorIndexTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode json(String raw) {
    try {
      return MAPPER.readTree(raw);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nested
  class ApplyIndexingOptions {

    @Test
    @DisplayName("null / JSON null leaves the options untouched")
    void nullValueIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, null);
      ApiVectorIndex.applyIndexingOptions(options, JsonNodeFactory.instance.nullNode());

      assertThat(options).isEmpty();
    }

    @Test
    @DisplayName("empty object leaves the options untouched")
    void emptyObjectIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, json("{}"));

      assertThat(options).isEmpty();
    }

    @Test
    @DisplayName("a profile name expands to its CQL options")
    void profileExpands() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options, JsonNodeFactory.instance.textNode("small-high-recall"));

      assertThat(options)
          .containsAllEntriesOf(VectorIndexProfiles.forName("small-high-recall").orElseThrow());
    }

    @Test
    @DisplayName("an unknown profile name throws UNKNOWN_VECTOR_INDEXING_PROFILE")
    void unknownProfileThrows() {
      var options = new HashMap<String, String>();

      assertThatThrownBy(
              () ->
                  ApiVectorIndex.applyIndexingOptions(
                      options, JsonNodeFactory.instance.textNode("no-such-profile")))
          .isInstanceOf(SchemaException.class)
          .satisfies(
              t ->
                  assertThat(((SchemaException) t).code)
                      .isEqualTo(SchemaException.Code.UNKNOWN_VECTOR_INDEXING_PROFILE.name()));
    }

    @Test
    @DisplayName("raw options are passed through, non-text values serialised to Strings")
    void rawOptionsPassThrough() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options,
          json(
              """
              {
                "maximum_node_connections": 32,
                "enable_hierarchy": true,
                "alpha": "1.2"
              }
              """));

      assertThat(options)
          .containsEntry("maximum_node_connections", "32")
          .containsEntry("enable_hierarchy", "true")
          .containsEntry("alpha", "1.2");
    }

    @Test
    @DisplayName("raw options merge with options already present")
    void rawOptionsMergeWithExisting() {
      var options = new HashMap<String, String>();
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");

      ApiVectorIndex.applyIndexingOptions(options, json("{\"maximum_node_connections\": 16}"));

      assertThat(options)
          .containsEntry(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER")
          .containsEntry("maximum_node_connections", "16");
    }

    @Test
    @DisplayName("a reserved option inside raw options throws INVALID_VECTOR_INDEXING_OPTIONS")
    void reservedOptionThrows() {
      var options = new HashMap<String, String>();

      assertThatThrownBy(
              () ->
                  ApiVectorIndex.applyIndexingOptions(
                      options, json("{\"similarity_function\": \"COSINE\"}")))
          .isInstanceOf(SchemaException.class)
          .satisfies(
              t ->
                  assertThat(((SchemaException) t).code)
                      .isEqualTo(SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS.name()));
    }

    @Test
    @DisplayName("a value that is neither String nor Object throws INVALID_VECTOR_INDEXING_OPTIONS")
    void wrongTypeThrows() {
      var options = new HashMap<String, String>();

      assertThatThrownBy(() -> ApiVectorIndex.applyIndexingOptions(options, json("[1, 2, 3]")))
          .isInstanceOf(SchemaException.class)
          .satisfies(
              t ->
                  assertThat(((SchemaException) t).code)
                      .isEqualTo(SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS.name()));
    }
  }

  @Nested
  class RenderIndexingOptions {

    @Test
    @DisplayName("returns null when only structural and dedicated-field options are present")
    void nullWhenNoTuningOptions() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");
      options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");

      assertThat(ApiVectorIndex.renderIndexingOptions(options)).isNull();
    }

    @Test
    @DisplayName(
        "returns only the tuning options, excluding structural and dedicated-field options")
    void rendersOnlyTuningOptions() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OPENAI_V3_SMALL");
      options.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32");

      var rendered = ApiVectorIndex.renderIndexingOptions(options);

      assertThat(rendered).isNotNull();
      assertThat(rendered.size()).isEqualTo(1);
      assertThat(rendered.get(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS).asText())
          .isEqualTo("32");
    }

    @Test
    @DisplayName("empty map renders null")
    void emptyMapRendersNull() {
      assertThat(ApiVectorIndex.renderIndexingOptions(Map.of())).isNull();
    }
  }
}
