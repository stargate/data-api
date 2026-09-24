package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link McpClauseSchemaCustomizer}. Verifies that user-facing JSON clause types are
 * advertised as plain JSON objects in MCP tool schemas, instead of leaking their internal Java
 * shape (e.g. {@code filterClause} / {@code json} for {@link FilterDefinition}), which the server
 * cannot deserialize.
 */
class McpClauseSchemaCustomizerTest {

  static Stream<Arguments> clauseTypesAndLeakedProperties() {
    return Stream.of(
        Arguments.of(FilterDefinition.class, new String[] {"filterClause", "json"}),
        Arguments.of(SortDefinition.class, new String[] {"sortClause", "json"}),
        Arguments.of(UpdateClause.class, new String[] {"updateOperationDefs"}),
        Arguments.of(
            FindAndRerankSort.class,
            new String[] {"commandFeatures", "lexicalSort", "vectorSort", "vectorizeSort"}));
  }

  @ParameterizedTest
  @MethodSource("clauseTypesAndLeakedProperties")
  void shouldAdvertiseClauseTypeAsPlainObject(Class<?> clauseType, String[] leakedProperties) {
    JsonNode schema = generateSchema(clauseType);

    assertEquals("object", schema.path("type").asText(), "Schema type should be a plain object");
    assertFalse(
        schema.path("description").asText().isBlank(),
        "Schema should carry a description of the Data API clause syntax");

    JsonNode properties = schema.get("properties");
    for (String leaked : leakedProperties) {
      assertTrue(
          properties == null || properties.get(leaked) == null,
          "Internal Java field '%s' must not leak into the advertised schema".formatted(leaked));
    }
  }

  @ParameterizedTest
  @MethodSource("clauseTypesAndLeakedProperties")
  void shouldEmbedValidJsonExample(Class<?> clauseType, String[] ignored) {
    JsonNode schema = generateSchema(clauseType);

    JsonNode examples = schema.get("examples");
    assertNotNull(examples, "Schema should embed an example of the accepted clause syntax");
    assertTrue(examples.isArray() && !examples.isEmpty(), "examples should be a non-empty array");
    assertTrue(examples.get(0).isObject(), "Example should be a JSON object");
  }

  @Test
  void shouldNotAffectRegularTypes() {
    record RegularOptions(Boolean ifNotExists) {}

    JsonNode schema = generateSchema(RegularOptions.class);

    assertNotNull(
        schema.path("properties").get("ifNotExists"),
        "Regular types should still expose their fields");
  }

  /** A {@link JsonDefinition} subtype without an explicit entry in the customizer's registry. */
  private static class UnregisteredDefinition
      extends io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition<Object> {
    UnregisteredDefinition(com.fasterxml.jackson.databind.JsonNode json) {
      super(json);
    }

    @Override
    public Object build(io.stargate.sgv2.jsonapi.api.model.command.CommandContext<?> ctx) {
      return null;
    }
  }

  @Test
  void shouldFallBackToGenericPlainObjectForUnregisteredJsonDefinitions() {
    JsonNode schema = generateSchema(UnregisteredDefinition.class);

    assertEquals("object", schema.path("type").asText(), "Schema type should be a plain object");
    assertFalse(
        schema.path("description").asText().isBlank(),
        "Fallback schema should carry a generic description");
    JsonNode properties = schema.get("properties");
    assertTrue(
        properties == null || properties.get("json") == null,
        "Internal field json must not leak into the fallback schema");
    assertNull(schema.get("examples"), "Fallback schema has no example to advertise");
  }

  /**
   * Generates a schema with a {@link SchemaGeneratorConfigBuilder} matching the configuration used
   * by Quarkus MCP Server's {@code DefaultSchemaGenerator}, with the customizer under test applied.
   */
  private static JsonNode generateSchema(Class<?> type) {
    var configBuilder =
        new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
            .without(Option.SCHEMA_VERSION_INDICATOR);
    new McpClauseSchemaCustomizer().customize(configBuilder);
    return new SchemaGenerator(configBuilder.build()).generateSchema(type);
  }
}
