package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.junit.jupiter.api.Test;

/**
 * Checks description resolution and reuse of independent schema copies in {@link
 * McpSchemaCustomizer}.
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

  @Schema(implementation = CountingSchemaProvider.class)
  record TestCachedType(String value) {}

  record TestCachedFields(
      @Schema(implementation = CountingSchemaProvider.class, description = "First field")
          TestCachedType first,
      @Schema(implementation = CountingSchemaProvider.class, description = "Second field")
          TestCachedType second) {}

  @Schema(name = "CustomizerTestSchema")
  public static class CountingSchemaProvider implements ApiSchemaProvider {
    static final AtomicInteger instances = new AtomicInteger();
    static final AtomicInteger schemasBuilt = new AtomicInteger();

    public CountingSchemaProvider() {
      instances.incrementAndGet();
    }

    @Override
    public org.eclipse.microprofile.openapi.models.media.Schema schema() {
      schemasBuilt.incrementAndGet();
      return ApiSchemaProvider.type(
              org.eclipse.microprofile.openapi.models.media.Schema.SchemaType.OBJECT)
          .addProperty(
              "value",
              ApiSchemaProvider.type(
                  org.eclipse.microprofile.openapi.models.media.Schema.SchemaType.STRING));
    }
  }

  @Test
  void shouldCacheSchemaAcrossTypesAndFields() {
    CountingSchemaProvider.instances.set(0);
    CountingSchemaProvider.schemasBuilt.set(0);
    var customizer = new McpSchemaCustomizer();

    schemaFor(customizer, TestCachedType.class);
    schemaFor(customizer, TestCachedFields.class);
    schemaFor(customizer, TestCachedType.class);

    assertEquals(1, CountingSchemaProvider.instances.get());
    assertEquals(1, CountingSchemaProvider.schemasBuilt.get());
  }

  @Test
  void shouldIsolateCachedSchemaFromGeneratedNodes() {
    var customizer = new McpSchemaCustomizer();
    var schema = schemaFor(customizer, TestCachedType.class);
    var original = schema.deepCopy();
    ((ObjectNode) schema.path("properties").path("value")).put("type", "integer");

    assertEquals(original, schemaFor(customizer, TestCachedType.class));
  }

  @Test
  void shouldKeepFieldDescriptionsIndependentWhenSharingCachedSchema() {
    var customizer = new McpSchemaCustomizer();
    var properties = schemaFor(customizer, TestCachedFields.class).path("properties");

    assertEquals("First field", properties.path("first").path("description").asText());
    assertEquals("Second field", properties.path("second").path("description").asText());
    assertFalse(schemaFor(customizer, TestCachedType.class).has("description"));
  }

  private static JsonNode schemaFor(McpSchemaCustomizer customizer, Class<?> type) {
    var builder = baseConfigBuilder();
    customizer.customize(builder);
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
