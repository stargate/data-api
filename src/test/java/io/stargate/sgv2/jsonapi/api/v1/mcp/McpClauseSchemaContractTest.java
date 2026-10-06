package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.*;
import io.quarkiverse.mcp.server.GlobalInputSchemaGenerator;
import io.quarkiverse.mcp.server.ToolManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Contracts between advertised MCP schemas and the actual Data API JSON deserializers. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpClauseSchemaContractTest {
  @Inject ObjectMapper objectMapper;
  @Inject GlobalInputSchemaGenerator schemaGenerator;
  @Inject ToolManager toolManager;

  record WireNames(
      @JsonProperty("wireName") String internalName, CommandFeatures commandFeatures) {}

  @Test
  void fieldsUseJsonPropertyNamesAndHideCommandFeatures() {
    var properties = schemaFor(WireNames.class).path("properties");
    assertTrue(properties.has("wireName"));
    assertFalse(properties.has("internalName"));
    assertFalse(properties.has("commandFeatures"));
  }

  @ParameterizedTest
  @MethodSource("clauseTypes")
  void everyAdvertisedClauseExampleDeserializes(Class<?> clauseType) throws Exception {
    var schema = schemaFor(clauseType);
    assertFalse(schema.toString().contains("commandFeatures"));
    assertFalse(schema.toString().contains("filterClause"));
    assertFalse(schema.toString().contains("sortClause"));
    assertFalse(schema.toString().contains("vectorizeSort"));
    var examples = schema.get("examples");
    assertNotNull(examples, "Clause schema must advertise examples of the accepted JSON");
    assertFalse(examples.isEmpty());
    for (var example : examples) {
      assertNotNull(objectMapper.treeToValue(example, clauseType));
    }
  }

  static Stream<Class<?>> clauseTypes() {
    return Stream.of(
        FilterDefinition.class,
        SortDefinition.class,
        FindAndRerankSort.class,
        FindAndRerankCommand.HybridLimits.class);
  }

  @Test
  void rerankToolAdvertisesWireFormatAndOverrideName() throws Exception {
    var schema = inputSchema("findAndRerank");
    var properties = schema.path("properties");
    assertTrue(properties.path("sort").path("properties").has("$hybrid"));
    var options = properties.path("options").path("properties");
    assertTrue(options.has("rerank"));
    assertFalse(options.has("rerankServiceOverride"));
    assertTrue(options.path("hybridLimits").has("anyOf"));
    assertFalse(schema.toString().contains("commandFeatures"));
    assertFalse(schema.toString().contains("vectorLimit"));

    for (var example : properties.path("sort").path("examples")) {
      assertNotNull(objectMapper.treeToValue(example, FindAndRerankSort.class));
    }
    for (var example : options.path("hybridLimits").path("examples")) {
      assertNotNull(objectMapper.treeToValue(example, FindAndRerankCommand.HybridLimits.class));
    }
  }

  @Test
  void findToolAdvertisesPlainFilterAndSortClauses() throws Exception {
    var properties = inputSchema("find").path("properties");
    var filter = properties.path("filter");
    var sort = properties.path("sort");
    assertFalse(filter.has("properties"));
    assertFalse(sort.toString().contains("sortClause"));
    assertTrue(filter.path("examples").size() > 0);
    assertTrue(sort.path("examples").size() > 0);
    var filterExample = filter.path("examples").get(0);
    var sortExample = sort.path("examples").get(0);
    assertEquals(
        filterExample, objectMapper.treeToValue(filterExample, FilterDefinition.class).json());
    assertEquals(sortExample, objectMapper.treeToValue(sortExample, SortDefinition.class).json());
  }

  @Test
  void openApiSortExamplesAreValidClauseJson() throws Exception {
    var schema = FindAndRerankSort.class.getAnnotation(Schema.class);
    for (var example : schema.examples()) {
      assertNotNull(objectMapper.readValue(example, FindAndRerankSort.class));
    }
  }

  private JsonNode inputSchema(String name) throws Exception {
    var tool = toolManager.getTool(name);
    assertNotNull(tool);
    return objectMapper.readTree(schemaGenerator.generate(tool).asJson());
  }

  private JsonNode schemaFor(Class<?> type) {
    var builder =
        new SchemaGeneratorConfigBuilder(
                objectMapper, SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
            .without(Option.SCHEMA_VERSION_INDICATOR);
    new McpSchemaDescriptionCustomizer().customize(builder);
    return new SchemaGenerator(builder.build()).generateSchema(type);
  }
}
