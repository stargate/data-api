package io.stargate.sgv2.jsonapi.service.schema.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.VectorIndexDefinitionDesc.VectorIndexDescOptions;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.VectorIndexDefinitionDesc.VectorIndexingDesc;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for the structured {@code vectorIndexing} ({@code {profile, options}}) handling on
 * {@link ApiVectorIndex}: that a request body deserializes to the expected object, how it is
 * validated and turned into the CQL index options map, and how it is described back. Deterministic;
 * needs no database (end-to-end also depends on the backend allowing custom SAI parameters).
 */
class ApiVectorIndexTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static VectorIndexingDesc vi(String profile, Map<String, Object> options) {
    return new VectorIndexingDesc(profile, options);
  }

  /** Source of every option that has a dedicated field and so is rejected inside options. */
  static Stream<String> reservedOptions() {
    return VectorConstants.CQLAnnIndex.RESERVED_OPTIONS.stream();
  }

  /** The request body deserializes into the expected {@code vectorIndexing} object. */
  @Nested
  class RequestShape {

    @Test
    void deserializesProfileAndOptions() throws Exception {
      var opts =
          MAPPER.readValue(
              """
              {
                "vectorIndexing": {
                  "profile": "small-high-recall",
                  "options": { "maximum_node_connections": 32, "enable_hierarchy": true }
                }
              }
              """,
              VectorIndexDescOptions.class);

      assertThat(opts.vectorIndexing()).isNotNull();
      assertThat(opts.vectorIndexing().profile()).isEqualTo("small-high-recall");
      assertThat(opts.vectorIndexing().options())
          .containsEntry("maximum_node_connections", 32)
          .containsEntry("enable_hierarchy", true);
    }

    @Test
    void deserializesProfileOnly() throws Exception {
      var opts =
          MAPPER.readValue(
              "{\"vectorIndexing\": {\"profile\": \"big-low-latency\"}}",
              VectorIndexDescOptions.class);

      assertThat(opts.vectorIndexing().profile()).isEqualTo("big-low-latency");
      assertThat(opts.vectorIndexing().options()).isNull();
    }

    @Test
    void absentVectorIndexingIsNull() throws Exception {
      var opts = MAPPER.readValue("{\"metric\": \"cosine\"}", VectorIndexDescOptions.class);

      assertThat(opts.vectorIndexing()).isNull();
    }
  }

  /** A {@code vectorIndexing} object resolves to the expected CQL index options map. */
  @Nested
  class ApplyIndexingOptions {

    @Test
    void nullIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, null);

      assertThat(options).isEmpty();
    }

    @Test
    void emptyDescIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, vi(null, null));
      ApiVectorIndex.applyIndexingOptions(options, vi(null, Map.of()));

      assertThat(options).isEmpty();
    }

    @Test
    void profileExpands() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, vi("small-high-recall", null));

      assertThat(options)
          .containsAllEntriesOf(VectorIndexProfiles.forName("small-high-recall").orElseThrow());
    }

    @Test
    void optionsApplied() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options,
          vi(
              null,
              Map.of("maximum_node_connections", 32, "enable_hierarchy", true, "alpha", "1.2")));

      assertThat(options)
          .containsEntry("maximum_node_connections", "32")
          .containsEntry("enable_hierarchy", "true")
          .containsEntry("alpha", "1.2");
    }

    @Test
    void optionsOverrideProfile() {
      var options = new HashMap<String, String>();

      // small-high-recall sets maximum_node_connections=32, construction_beam_width=200
      ApiVectorIndex.applyIndexingOptions(
          options, vi("small-high-recall", Map.of("maximum_node_connections", 99)));

      assertThat(options)
          .containsEntry("maximum_node_connections", "99") // explicit option wins
          .containsEntry("construction_beam_width", "200"); // inherited from the profile
    }

    @Test
    void mergesWithExistingOptions() {
      var options = new HashMap<String, String>();
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");

      ApiVectorIndex.applyIndexingOptions(
          options, vi(null, Map.of("maximum_node_connections", 16)));

      assertThat(options)
          .containsEntry(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER")
          .containsEntry("maximum_node_connections", "16");
    }

    @Test
    void allAllowedOptionsAccepted() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options,
          vi(
              null,
              Map.of(
                  "maximum_node_connections",
                  16,
                  "construction_beam_width",
                  100,
                  "neighborhood_overflow",
                  1.2,
                  "alpha",
                  1.2,
                  "enable_hierarchy",
                  true)));

      assertThat(options.keySet())
          .containsExactlyInAnyOrderElementsOf(VectorConstants.CQLAnnIndex.ALLOWED_OPTIONS);
    }

    @Test
    void unknownProfileThrows() {
      assertSchemaError(
          vi("no-such-profile", null), SchemaException.Code.UNKNOWN_VECTOR_INDEXING_PROFILE);
    }

    @Test
    void blankProfileThrows() {
      assertSchemaError(vi("", null), SchemaException.Code.UNKNOWN_VECTOR_INDEXING_PROFILE);
    }

    @ParameterizedTest
    @MethodSource(
        "io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndexTest#reservedOptions")
    void reservedOptionThrows(String reservedOption) {
      assertSchemaError(
          vi(null, Map.of(reservedOption, "x")),
          SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
    }

    @ParameterizedTest
    @ValueSource(strings = {"class_name", "target", "optimize_for", "bogus_option"})
    void unsupportedOptionThrows(String optionName) {
      assertSchemaError(
          vi(null, Map.of(optionName, "x")), SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
    }

    @Test
    void numericOptionsUsePlainString() {
      var options = new HashMap<String, String>();

      // JSON numbers arrive as BigDecimal; the CQL value must not use scientific notation.
      ApiVectorIndex.applyIndexingOptions(
          options,
          vi(
              null,
              Map.of(
                  "construction_beam_width", new BigDecimal("1E+2"),
                  "alpha", new BigDecimal("1.5"))));

      assertThat(options)
          .containsEntry("construction_beam_width", "100")
          .containsEntry("alpha", "1.5");
    }

    @Test
    void nonScalarOptionValueThrows() {
      // "alpha" is an allowed key, so this reaches the scalar-value check
      assertSchemaError(
          vi(null, Map.of("alpha", List.of(1, 2))),
          SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
      assertSchemaError(
          vi(null, Map.of("alpha", Map.of("x", 1))),
          SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
    }

    private void assertSchemaError(VectorIndexingDesc desc, SchemaException.Code code) {
      var options = new HashMap<String, String>();
      assertThatThrownBy(() -> ApiVectorIndex.applyIndexingOptions(options, desc))
          .isInstanceOf(SchemaException.class)
          .satisfies(t -> assertThat(((SchemaException) t).code).isEqualTo(code.name()));
    }
  }

  /** The CQL index options map describes back to the expected {@code vectorIndexing} object. */
  @Nested
  class DescribeIndexingOptions {

    @Test
    void nullWhenNoTuningOptions() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");
      options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");

      assertThat(ApiVectorIndex.describeIndexingOptions(options)).isNull();
    }

    @Test
    void describesTuningOptionsUnderOptions() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OPENAI_V3_SMALL");
      options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");
      options.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32");
      options.put(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200");

      var described = ApiVectorIndex.describeIndexingOptions(options);

      assertThat(described).isNotNull();
      assertThat(described.profile()).isNull();
      assertThat(described.options())
          .containsOnly(
              entry(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32"),
              entry(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200"));
    }

    @Test
    void omitsNonAllowlistedKeys() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");
      options.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "16");
      // a real SAI option the API does not manage (e.g. set directly via CQL); not surfaced
      options.put("optimize_for", "recall");

      var described = ApiVectorIndex.describeIndexingOptions(options);

      assertThat(described).isNotNull();
      assertThat(described.options())
          .containsOnlyKeys(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS);
    }

    @Test
    void emptyMapDescribesNull() {
      assertThat(ApiVectorIndex.describeIndexingOptions(Map.of())).isNull();
    }
  }

  /** Applying options then describing them round-trips the tuning options. */
  @Nested
  class RoundTrip {

    @Test
    void applyThenDescribe() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options, vi(null, Map.of("maximum_node_connections", 32, "alpha", 1.2)));
      var described = ApiVectorIndex.describeIndexingOptions(options);

      assertThat(described).isNotNull();
      assertThat(described.options())
          .containsOnly(
              entry(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32"),
              entry(VectorConstants.CQLAnnIndex.ALPHA, "1.2"));
    }
  }
}
