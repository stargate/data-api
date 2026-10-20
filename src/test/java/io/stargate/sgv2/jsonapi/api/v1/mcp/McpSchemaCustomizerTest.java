package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.junit.jupiter.api.Test;

/**
 * Checks OpenAPI references and annotation descriptions supplied by {@link McpSchemaCustomizer}.
 */
class McpSchemaCustomizerTest {

  /**
   * Test record that mimics nested command structures such as {@link
   * io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand}. CreateTableCommand →
   * TableDefinitionDesc → PrimaryKeyDesc
   */
  record TestPrimaryKey(
      @Schema(description = "Partition key columns") String[] keys,
      @Schema(description = "Clustering key ordering") String ordering) {}

  record TestDefinition(
      @Schema(description = "Column definitions for the table") String columns,
      @Schema(description = "Primary key definition") TestPrimaryKey primaryKey) {}

  record TestOptions(
      @Schema(description = "Flag to ignore if already exists") Boolean ifNotExists) {}

  record TestCreateTable(
      @Schema(description = "Table definition") TestDefinition definition,
      @Schema(description = "Configuration options") TestOptions options,
      Boolean noAnnotation) {}

  @Schema(description = "Reusable value description")
  record TestDescribedType(String value) {}

  record TestTypeDescription(TestDescribedType value) {}

  record TestHiddenFields(@Schema(hidden = true) String internal, String visible) {}

  record TestNamedField(@Schema(name = "aPropertyName") String value) {}

  @Schema(name = "CustomizerTestSchema")
  record TestSharedType(String internalValue) {}

  @Schema(ref = "#/components/schemas/CustomizerTestSchema")
  record TestReferencedType(String internalValue) {}

  @Schema(name = "CustomizerTestSchema")
  static class SharedSchema {}

  record TestImplementationValue(String internalValue) {}

  record TestSharedFields(
      @Schema(implementation = SharedSchema.class, description = "First field")
          TestImplementationValue first,
      @Schema(implementation = SharedSchema.class, description = "Second field")
          TestImplementationValue second) {}

  @Test
  void shouldReferenceNamedComponentInsteadOfInternalJavaFields() {
    var schema = schemaFor(TestSharedType.class);

    assertEquals("#/components/schemas/CustomizerTestSchema", schema.path("$ref").asText());
    assertFalse(schema.path("properties").has("internalValue"));
  }

  @Test
  void shouldKeepExplicitComponentReference() {
    assertEquals(schemaFor(TestSharedType.class), schemaFor(TestReferencedType.class));
  }

  @Test
  void shouldReferenceFieldImplementationAndKeepFieldDescriptions() {
    var properties = schemaFor(TestSharedFields.class).path("properties");

    for (var name : new String[] {"first", "second"}) {
      assertEquals(
          "#/components/schemas/CustomizerTestSchema", properties.path(name).path("$ref").asText());
      assertFalse(properties.path(name).path("properties").has("internalValue"));
    }
    assertEquals("First field", schemaValue(properties.path("first"), "description").asText());
    assertEquals("Second field", schemaValue(properties.path("second"), "description").asText());
    assertFalse(schemaFor(TestSharedType.class).has("description"));
  }

  @Test
  void shouldOmitFieldsHiddenBySchemaAnnotation() {
    var properties = schemaFor(TestHiddenFields.class).path("properties");

    assertFalse(properties.has("internal"));
    assertTrue(properties.has("visible"));
  }

  @Test
  void shouldNotTreatFieldSchemaNameAsComponentName() {
    var schema = assertDoesNotThrow(() -> schemaFor(TestNamedField.class));

    assertEquals("string", schema.path("properties").path("value").path("type").asText());
  }

  private static JsonNode schemaFor(Class<?> type) {
    var builder = baseConfigBuilder();
    new McpSchemaCustomizer().customize(builder);
    return new SchemaGenerator(builder.build()).generateSchema(type);
  }

  @Test
  void shouldIncludeRootTypeDescription() {
    var configBuilder = baseConfigBuilder();
    new McpSchemaCustomizer().customize(configBuilder);

    var schema = new SchemaGenerator(configBuilder.build()).generateSchema(TestDescribedType.class);

    assertEquals("Reusable value description", schema.path("description").asText());
  }

  @Test
  void shouldIncludeNestedTypeDescriptionWithoutFieldDescription() {
    var configBuilder = baseConfigBuilder();
    new McpSchemaCustomizer().customize(configBuilder);

    var schema =
        new SchemaGenerator(configBuilder.build()).generateSchema(TestTypeDescription.class);

    assertEquals(
        "Reusable value description",
        schema.path("properties").path("value").path("description").asText());
  }

  @Test
  void shouldIncludeSchemaDescriptionWithCustomizer() {
    var configBuilder = baseConfigBuilder();
    new McpSchemaCustomizer().customize(configBuilder);

    var schema = new SchemaGenerator(configBuilder.build()).generateSchema(TestCreateTable.class);
    var props = schemaValue(schema, "properties");

    // Level 1: top-level fields
    assertEquals("Table definition", schemaValue(props.path("definition"), "description").asText());
    assertEquals(
        "Configuration options", schemaValue(props.path("options"), "description").asText());
    assertTrue(
        schemaValue(props.path("noAnnotation"), "description").isMissingNode(),
        "Fields without @Schema should have no description");

    // Level 2: nested inside TestDefinition
    var defProps = schemaValue(props.path("definition"), "properties");
    assertEquals(
        "Column definitions for the table",
        schemaValue(defProps.path("columns"), "description").asText());
    assertEquals(
        "Primary key definition", schemaValue(defProps.path("primaryKey"), "description").asText());

    // Level 2: nested inside TestOptions
    var optProps = schemaValue(props.path("options"), "properties");
    assertEquals(
        "Flag to ignore if already exists",
        schemaValue(optProps.path("ifNotExists"), "description").asText());

    // Level 3: nested inside TestPrimaryKey
    var pkProps = schemaValue(defProps.path("primaryKey"), "properties");
    assertEquals(
        "Partition key columns", schemaValue(pkProps.path("keys"), "description").asText());
    assertEquals(
        "Clustering key ordering", schemaValue(pkProps.path("ordering"), "description").asText());
  }

  /** Metadata may accompany a schema in a separate allOf branch without merging its constraints. */
  private static JsonNode schemaValue(JsonNode schema, String keyword) {
    if (schema.has(keyword)) {
      return schema.get(keyword);
    }
    for (var branch : schema.path("allOf")) {
      var value = schemaValue(branch, keyword);
      if (!value.isMissingNode()) {
        return value;
      }
    }
    return MissingNode.getInstance();
  }

  @Test
  void shouldNotIncludeDescriptionWithoutCustomizer() {
    var schema =
        new SchemaGenerator(baseConfigBuilder().build()).generateSchema(TestCreateTable.class);

    // Spot-check: a deeply nested field should have no description
    var pkProps =
        schema
            .get("properties")
            .get("definition")
            .get("properties")
            .get("primaryKey")
            .get("properties");
    assertNull(
        pkProps.get("keys").get("description"),
        "Without customizer, PLAIN_JSON preset should not resolve @Schema descriptions");
  }

  /**
   * Creates a {@link SchemaGeneratorConfigBuilder} matching the configuration used by Quarkus MCP
   * Server's {@code DefaultSchemaGenerator}.
   */
  private static SchemaGeneratorConfigBuilder baseConfigBuilder() {
    return new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
        .without(Option.SCHEMA_VERSION_INDICATOR);
  }
}
