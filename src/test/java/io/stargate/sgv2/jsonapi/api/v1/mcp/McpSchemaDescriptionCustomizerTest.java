package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link McpSchemaDescriptionCustomizer}. Verifies that the customizer correctly
 * resolves {@link Schema#description()} into the generated JSON schema.
 */
class McpSchemaDescriptionCustomizerTest {

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

  @Test
  void shouldIncludeSchemaDescriptionWithCustomizer() {
    var configBuilder = baseConfigBuilder();
    new McpSchemaDescriptionCustomizer().customize(configBuilder);

    var schema = new SchemaGenerator(configBuilder.build()).generateSchema(TestCreateTable.class);
    var props = schema.get("properties");

    // Level 1: top-level fields
    assertEquals("Table definition", props.get("definition").get("description").asText());
    assertEquals("Configuration options", props.get("options").get("description").asText());
    assertNull(
        props.get("noAnnotation").get("description"),
        "Fields without @Schema should have no description");

    // Level 2: nested inside TestDefinition
    var defProps = props.get("definition").get("properties");
    assertEquals(
        "Column definitions for the table", defProps.get("columns").get("description").asText());
    assertEquals("Primary key definition", defProps.get("primaryKey").get("description").asText());

    // Level 2: nested inside TestOptions
    var optProps = props.get("options").get("properties");
    assertEquals(
        "Flag to ignore if already exists",
        optProps.get("ifNotExists").get("description").asText());

    // Level 3: nested inside TestPrimaryKey
    var pkProps = defProps.get("primaryKey").get("properties");
    assertEquals("Partition key columns", pkProps.get("keys").get("description").asText());
    assertEquals("Clustering key ordering", pkProps.get("ordering").get("description").asText());
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
