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
 * Unit tests for the overloaded {@code vectorIndexing} on {@link ApiVectorIndex}, where the value
 * is either a profile name string or a raw SAI options object.
 *
 * <p>Covers deserialization of a request body, validation, the resulting CQL index options map, and
 * the describe-back. Needs no database (end-to-end also depends on the backend allowing custom SAI
 * parameters).
 */
class ApiVectorIndexTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static VectorIndexingDesc profile(String profile) {
    return VectorIndexingDesc.ofProfile(profile);
  }

  private static VectorIndexingDesc options(Map<String, Object> options) {
    return VectorIndexingDesc.ofOptions(options);
  }

  /** Options with a dedicated field, so rejected inside options. */
  static Stream<String> reservedOptions() {
    return VectorConstants.CQLAnnIndex.RESERVED_OPTIONS.stream();
  }

  /**
   * The overloaded {@code vectorIndexing} deserializes by JSON type: a string is a profile, an
   * object is raw options, and anything else is rejected.
   */
  @Nested
  class RequestShape {

    @Test
    void stringDeserializesToProfile() throws Exception {
      var opts =
          MAPPER.readValue(
              "{\"vectorIndexing\": \"small-high-recall\"}", VectorIndexDescOptions.class);

      assertThat(opts.vectorIndexing()).isNotNull();
      assertThat(opts.vectorIndexing().profile()).isEqualTo("small-high-recall");
      assertThat(opts.vectorIndexing().options()).isNull();
    }

    @Test
    void objectDeserializesToRawOptions() throws Exception {
      var opts =
          MAPPER.readValue(
              """
              {
                "vectorIndexing": { "maximum_node_connections": 32, "enable_hierarchy": true }
              }
              """,
              VectorIndexDescOptions.class);

      assertThat(opts.vectorIndexing()).isNotNull();
      assertThat(opts.vectorIndexing().profile()).isNull();
      assertThat(opts.vectorIndexing().options())
          .containsEntry("maximum_node_connections", 32)
          .containsEntry("enable_hierarchy", true);
    }

    @Test
    void absentVectorIndexingIsNull() throws Exception {
      var opts = MAPPER.readValue("{\"metric\": \"cosine\"}", VectorIndexDescOptions.class);

      assertThat(opts.vectorIndexing()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"123", "true", "[\"small-high-recall\"]"})
    void nonStringNonObjectRejected(String value) {
      // Jackson may surface the deserializer's SchemaException directly or wrapped, so assert one
      // with the expected code is somewhere in the cause chain.
      assertThatThrownBy(
              () ->
                  MAPPER.readValue(
                      "{\"vectorIndexing\": " + value + "}", VectorIndexDescOptions.class))
          .satisfies(
              t -> {
                var schemaException = findSchemaException(t);
                assertThat(schemaException).as("a SchemaException in the cause chain").isNotNull();
                assertThat(schemaException.code)
                    .isEqualTo(SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS.name());
              });
    }

    private SchemaException findSchemaException(Throwable t) {
      for (Throwable cause = t; cause != null; cause = cause.getCause()) {
        if (cause instanceof SchemaException schemaException) {
          return schemaException;
        }
      }
      return null;
    }
  }

  /** A {@code vectorIndexing} value resolves to the expected CQL index options map. */
  @Nested
  class ApplyIndexingOptions {

    @Test
    void nullIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, null);

      assertThat(options).isEmpty();
    }

    @Test
    void emptyOptionsIsNoOp() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, options(Map.of()));

      assertThat(options).isEmpty();
    }

    @Test
    void profileExpands() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(options, profile("small-high-recall"));

      assertThat(options)
          .containsAllEntriesOf(VectorIndexProfiles.forName("small-high-recall").orElseThrow());
    }

    @Test
    void optionsApplied() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options,
          options(
              Map.of("maximum_node_connections", 32, "enable_hierarchy", true, "alpha", "1.2")));

      assertThat(options)
          .containsEntry("maximum_node_connections", "32")
          .containsEntry("enable_hierarchy", "true")
          .containsEntry("alpha", "1.2");
    }

    @Test
    void mergesWithExistingOptions() {
      var options = new HashMap<String, String>();
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");

      ApiVectorIndex.applyIndexingOptions(options, options(Map.of("maximum_node_connections", 16)));

      assertThat(options)
          .containsEntry(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER")
          .containsEntry("maximum_node_connections", "16");
    }

    @Test
    void allAllowedOptionsAccepted() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options,
          options(
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
          profile("no-such-profile"), SchemaException.Code.UNKNOWN_VECTOR_INDEXING_PROFILE);
    }

    @Test
    void blankProfileThrows() {
      assertSchemaError(profile(""), SchemaException.Code.UNKNOWN_VECTOR_INDEXING_PROFILE);
    }

    @ParameterizedTest
    @MethodSource(
        "io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndexTest#reservedOptions")
    void reservedOptionThrows(String reservedOption) {
      assertSchemaError(
          options(Map.of(reservedOption, "x")),
          SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
    }

    @ParameterizedTest
    @ValueSource(strings = {"class_name", "target", "optimize_for", "bogus_option"})
    void unsupportedOptionThrows(String optionName) {
      assertSchemaError(
          options(Map.of(optionName, "x")), SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
    }

    @Test
    void numericOptionsUsePlainString() {
      var options = new HashMap<String, String>();

      // JSON numbers arrive as BigDecimal; the CQL value must not use scientific notation.
      ApiVectorIndex.applyIndexingOptions(
          options,
          options(
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
          options(Map.of("alpha", List.of(1, 2))),
          SchemaException.Code.INVALID_VECTOR_INDEXING_OPTIONS);
      assertSchemaError(
          options(Map.of("alpha", Map.of("x", 1))),
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
      // options that do not match any profile are echoed verbatim under options
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OPENAI_V3_SMALL");
      options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");
      options.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32");
      options.put(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "123");

      var described = ApiVectorIndex.describeIndexingOptions(options);

      assertThat(described).isNotNull();
      assertThat(described.profile()).isNull();
      assertThat(described.options())
          .containsOnly(
              entry(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32"),
              entry(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "123"));
    }

    @Test
    void detectsKnownProfileFromOptions() {
      // options that exactly match small-high-recall's expansion are echoed as the profile name
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(CQLSAIIndex.Options.TARGET, "my_vector");
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OPENAI_V3_SMALL");
      options.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");
      options.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32");
      options.put(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200");

      var described = ApiVectorIndex.describeIndexingOptions(options);

      assertThat(described).isNotNull();
      assertThat(described.profile()).isEqualTo("small-high-recall");
      assertThat(described.options()).isNull();
    }

    @Test
    void omitsNonAllowlistedKeys() {
      var options = new HashMap<String, String>();
      options.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      options.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");
      // a non-profile value, so the allow-listed key is echoed as raw options
      options.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "20");
      // a real SAI option the API does not manage (e.g. set directly via CQL), not surfaced
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

  /** tuningOptions keeps only the allow-listed tuning options, dropping reserved and structural. */
  @Nested
  class TuningOptionsFilter {

    @Test
    void keepsAllowlistedOptionsExcludingReservedAndStructural() {
      var indexOptions = new HashMap<String, String>();
      indexOptions.put(CQLSAIIndex.Options.CLASS_NAME, CQLSAIIndex.SAI_CLASS_NAME);
      indexOptions.put(CQLSAIIndex.Options.TARGET, "my_vector");
      indexOptions.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OPENAI_V3_SMALL");
      indexOptions.put(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION, "COSINE");
      // values are kept as-is; the filter only drops keys, it does not interpret them
      indexOptions.put(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "99");
      indexOptions.put(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200");

      assertThat(ApiVectorIndex.tuningOptions(indexOptions))
          .containsOnly(
              entry(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "99"),
              entry(VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200"));
    }

    @Test
    void emptyWhenNoTuningOptions() {
      var indexOptions = new HashMap<String, String>();
      indexOptions.put(VectorConstants.CQLAnnIndex.SOURCE_MODEL, "OTHER");

      assertThat(ApiVectorIndex.tuningOptions(indexOptions)).isEmpty();
    }
  }

  /** Applying options then describing them round-trips the tuning options. */
  @Nested
  class RoundTrip {

    @Test
    void applyThenDescribe() {
      var options = new HashMap<String, String>();

      ApiVectorIndex.applyIndexingOptions(
          options, options(Map.of("maximum_node_connections", 32, "alpha", 1.2)));
      var described = ApiVectorIndex.describeIndexingOptions(options);

      assertThat(described).isNotNull();
      assertThat(described.options())
          .containsOnly(
              entry(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32"),
              entry(VectorConstants.CQLAnnIndex.ALPHA, "1.2"));
    }
  }
}
