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
    void nullValueIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, null);
      ApiVectorIndex.applyIndexingOptions(options, JsonNodeFactory.instance.nullNode());

      assertThat(options).isEmpty();
    }

    @Test
    void emptyObjectIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, json("{}"));

      assertThat(options).isEmpty();
    }

    @Test
    void profileExpands() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options, JsonNodeFactory.instance.textNode("small-high-recall"));

      assertThat(options)
          .containsAllEntriesOf(VectorIndexProfiles.forName("small-high-recall").orElseThrow());
    }

    @Test
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
    void rawOptionsMergeWithExisting() {
      var options = new HashMap<String, String>();
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");

      ApiVectorIndex.applyIndexingOptions(options, json("{\"maximum_node_connections\": 16}"));

      assertThat(options)
          .containsEntry(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER")
          .containsEntry("maximum_node_connections", "16");
    }

    @Test
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
    void wrongTypeThrows() {
      var options = new HashMap<String, String>();

      assertThatThrownBy(() -> ApiVectorIndex.applyIndexingOptions(options, json("[1, 2, 3]")))
          .isInstanceOf(SchemaException.class)
          .satisfies(
              t ->
                  assertThat(((SchemaException) t).code)
                      .isEqualTo(SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS.name()));
    }

    @Test
    void structuralOptionThrows() {
      var options = new HashMap<String, String>();

      assertThatThrownBy(
              () -> ApiVectorIndex.applyIndexingOptions(options, json("{\"class_name\": \"x\"}")))
          .isInstanceOf(SchemaException.class)
          .satisfies(
              t ->
                  assertThat(((SchemaException) t).code)
                      .isEqualTo(SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS.name()));

      assertThatThrownBy(
              () -> ApiVectorIndex.applyIndexingOptions(options, json("{\"target\": \"y\"}")))
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
    void nullWhenNoTuningOptions() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");
      options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");

      assertThat(ApiVectorIndex.renderIndexingOptions(options)).isNull();
    }

    @Test
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
    void emptyMapRendersNull() {
      assertThat(ApiVectorIndex.renderIndexingOptions(Map.of())).isNull();
    }
  }
}
