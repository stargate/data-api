package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.*;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.quarkiverse.mcp.server.GlobalInputSchemaGenerator;
import io.quarkiverse.mcp.server.ToolManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl.RerankingProviderConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl.RerankingProviderConfigImpl.AuthenticationConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl.RequestPropertiesImpl;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks the JSON schemas the Data API publishes for command clauses such as filter, sort, and
 * update. Clients build requests from the MCP tool input schemas and the OpenAPI document, so both
 * must accept the JSON the server accepts and reject the JSON it rejects.
 *
 * <p>The clause classes use custom deserializers, so each schema is a hand-written {@code @Schema}
 * annotation rather than one generated from the Java fields. These tests compare each schema with
 * the server's parser so the two stay in step. No commands run and no database is needed.
 *
 * <p>Test cases are tables with one JSON value per line, separated from other columns by {@code |}.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpClauseSchemaContractTest {
  @Inject ObjectMapper objectMapper;
  @Inject GlobalInputSchemaGenerator schemaGenerator;
  @Inject ToolManager toolManager;
  @Inject Validator validator;

  // ---- Clause schemas, one clause type at a time ----

  /** The clause types whose schemas come from {@code @Schema} rather than their Java fields. */
  static Stream<Class<?>> clauseTypes() {
    return Stream.of(
        FilterDefinition.class,
        SortDefinition.class,
        UpdateClause.class,
        FindAndRerankSort.class,
        FindAndRerankCommand.HybridLimits.class);
  }

  /** Clients copy these examples, so each must pass its own schema and the server's parser. */
  @ParameterizedTest
  @MethodSource("clauseTypes")
  void everyAdvertisedClauseExampleDeserializes(Class<?> clauseType) throws Exception {
    var schema = schemaFor(clauseType);
    var examples = schemaMetadata(schema, schema, "examples");
    assertFalse(
        examples.isMissingNode(), "Clause schema must advertise examples of the accepted JSON");
    assertFalse(examples.isEmpty());
    for (var example : examples) {
      assertSchemaAccepts(schema, example);
      var clause = objectMapper.treeToValue(example, clauseType);
      assertNotNull(clause);
      // These clauses defer parsing until build, so deserialization alone cannot validate them.
      if (clause instanceof FilterDefinition filter) {
        assertNotNull(filter.build(new TestConstants().collectionContext()));
      } else if (clause instanceof SortDefinition sort) {
        assertNotNull(sort.build(new TestConstants().collectionContext()));
      } else if (clause instanceof UpdateClause update) {
        // $pullAll is table only, so the collection operations cannot build it.
        if (!example.has(UpdateOperator.PULL_ALL.apiName())) {
          assertNotNull(update.buildOperations());
        }
      }
    }
  }

  /**
   * MCP builds clause schemas from the OpenAPI document generated at build time. This checks they
   * match the document the server serves, so Swagger and MCP clients see the same clause JSON.
   */
  @ParameterizedTest
  @MethodSource("clauseTypes")
  void openApiAndMcpShareClauseSchemas(Class<?> clauseType) throws Exception {
    var name = clauseType.getAnnotation(Schema.class).name();
    var document = openApi();
    var openApiSchema = document.path("components").path("schemas").path(name);
    assertFalse(openApiSchema.isMissingNode(), "Missing OpenAPI component " + name);
    var expected =
        objectMapper.createObjectNode().put("$ref", McpOpenApiSchemaAdapter.componentRef(name));
    McpOpenApiSchemaAdapter.bundleOpenApiSchemas(expected, document);
    // Normalize numeric examples through the same mapper before comparing schema trees.
    var actual = objectMapper.readTree(schemaFor(clauseType).toString());
    assertEquals(referencedSchema(expected, expected), referencedSchema(actual, actual));
    assertEquals(expected.path("$defs"), actual.path("$defs"));
  }

  /** Accepting the examples proves nothing alone, since an empty schema accepts every value. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          FilterDefinition  | []
          FindAndRerankSort | {"vectorizeSort": "query"}
          FindAndRerankSort | {"$hybrid": {"$lexical": 1}}
          FindAndRerankSort | {"$hybrid": {"$vector": "query"}}
          FindAndRerankSort | {"$hybrid": {"$vector": null, "$vectorize": null}}
          HybridLimits      | {"vectorLimit": 50, "lexicalLimit": 10}
          HybridLimits      | {"$vector": 50}
          HybridLimits      | {"$vector": 50, "$lexical": "10"}
          """)
  void clauseSchemasRejectInvalidJson(String clauseName, String json) throws Exception {
    var clauseType =
        clauseTypes().filter(type -> type.getSimpleName().equals(clauseName)).findFirst();
    assertTrue(clauseType.isPresent(), () -> "Not a clause type: " + clauseName);
    assertSchemaRejects(schemaFor(clauseType.get()), objectMapper.readTree(json));
  }

  /** The findAndRerank sort parser accepts null and empty legs, so the schema must as well. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          {"$hybrid": {"$vector": null, "$lexical": "query"}}
          {"$hybrid": {"$vectorize": null, "$lexical": "query"}}
          {"$hybrid": {"$vectorize": "query", "$lexical": null}}
          {"$hybrid": {"$vector": [1, 2, 3], "$lexical": null}}
          {"$hybrid": {"$vector": null}}
          {"$hybrid": {"$vectorize": null}}
          {"$hybrid": {"$lexical": null}}
          {"$hybrid": {}}
          {}
          null
          """)
  void rerankSortAcceptsNullableAndEmptyJson(String json) throws Exception {
    var sort = objectMapper.readTree(json);
    assertSchemaAccepts(schemaFor(FindAndRerankSort.class), sort);
    var clause = assertDoesNotThrow(() -> objectMapper.treeToValue(sort, FindAndRerankSort.class));
    if (!sort.isNull()) {
      assertNotNull(clause);
    }
    assertSchemaAccepts(inputSchema("findAndRerank"), toolInput("{}").set("sort", sort));
  }

  /** The update clause examples are the only place a client sees how each operator is written. */
  @ParameterizedTest
  @EnumSource(UpdateOperator.class)
  void everyUpdateOperatorHasAnAdvertisedExample(UpdateOperator operator) {
    var schema = schemaFor(UpdateClause.class);
    var examples = schemaMetadata(schema, schema, "examples");
    assertTrue(
        StreamSupport.stream(examples.spliterator(), false)
            .anyMatch(example -> example.has(operator.apiName())),
        () -> "Missing example for " + operator.apiName());
  }

  /** Nonempty arguments exercise each operation's parser, including supported array modifiers. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          {}
          {"$set": {}}
          {"$set": {"profile.name": "Ada", "optional": null}}
          {"$set": {"created": {"$date": 123456}, "owner": {"$uuid": "550e8400-e29b-41d4-a716-446655440000"}}}
          {"$set": {"nested": [null, true, 1.25, {"name": "Ada"}]}, "$unset": {"obsolete": 1}}
          {"$unset": {"obsolete": null}}
          {"$setOnInsert": {"created": true}}
          {"$inc": {"score": 0.25}}
          {"$mul": {"score": 1.5}}
          {"$min": {"score": 2.5}}
          {"$max": {"score": 9.5}}
          {"$rename": {"oldName": "newName"}}
          {"$currentDate": {"updated": true, "reviewed": {"$type": "date"}}}
          {"$pop": {"first": -1, "last": 1}}
          {"$push": {"items": {"$each": [1, 2], "$position": 0}}}
          {"$addToSet": {"tags": {"$each": ["a", "b"]}}}
          """)
  void updateClauseAcceptsSupportedUpdates(String json) throws Exception {
    var update = objectMapper.readTree(json);
    assertSchemaAccepts(schemaFor(UpdateClause.class), update);
    assertNotNull(objectMapper.treeToValue(update, UpdateClause.class).buildOperations());
  }

  /** The update parser rejects these, so the schema must reject them too. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          []
          5
          "update"
          {"name": "Ada"}
          {"updateOperationDefs": {}}
          {"$unknown": {}}
          {"$set": null}
          {"$set": 5}
          {"$set": []}
          {"$set": "value"}
          """)
  void updateClauseRejectsMalformedUpdates(String json) throws Exception {
    var update = objectMapper.readTree(json);
    assertSchemaRejects(schemaFor(UpdateClause.class), update);
    assertThrows(UpdateException.class, () -> objectMapper.treeToValue(update, UpdateClause.class));
  }

  // ---- MCP tool input schemas, as the MCP server publishes them ----

  /**
   * Tool schemas describe the JSON clients send, so they must show JSON names and leave out fields
   * the server keeps for itself. Each name below is a Java field on a class a tool argument uses.
   */
  @Test
  void toolSchemasHideJavaFieldNames() throws Exception {
    var javaFieldNames =
        List.of(
            // @JsonIgnore fields on FilterDefinition, SortDefinition, and FindAndRerankSort.
            "filterClause",
            "sortClause",
            "commandFeatures",
            "rerankLegMode",
            "hybridSort",
            "vectorizeSort",
            "lexicalSort",
            "vectorSort",
            // Fields the JSON names differently, on UpdateClause, HybridLimits, and Options.
            "updateOperationDefs",
            "vectorLimit",
            "lexicalLimit",
            "rerankServiceOverride");
    for (var tool : toolManager) {
      var document = inputSchema(tool.name());
      for (var name : javaFieldNames) {
        assertNull(document.findParent(name), () -> tool.name() + " schema shows " + name);
      }
    }
  }

  /** MCP clients choose argument values from these descriptions, so a bare name is not enough. */
  @Test
  void everyToolDescribesEveryArgument() throws Exception {
    for (var tool : toolManager) {
      var document = inputSchema(tool.name());
      for (var argument : document.path("properties").properties()) {
        var label = tool.name() + "." + argument.getKey();
        var description = schemaMetadata(document, argument.getValue(), "description").asText();
        assertFalse(description.isBlank(), label);
        assertNotEquals(argument.getKey(), description.toLowerCase(), label);
      }
    }
  }

  /** An example at the end of an argument description must parse as that argument. */
  @Test
  void argumentDescriptionExamplesDeserialize() throws Exception {
    var marker = "for example ";
    // Trailing text after the example would mean the example is not the whole JSON value.
    var reader = objectMapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    var checked = 0;
    for (var tool : toolManager) {
      for (var argument : tool.arguments()) {
        var start = argument.description().indexOf(marker);
        if (start < 0) {
          continue;
        }
        var label = tool.name() + "." + argument.name();
        var example = reader.readTree(argument.description().substring(start + marker.length()));
        var type = objectMapper.constructType(argument.type());
        assertNotNull(
            assertDoesNotThrow(() -> objectMapper.treeToValue(example, type), label), label);
        checked++;
      }
    }
    assertTrue(checked > 0, "No argument description has an example");
  }

  /**
   * Each update tool must use the update clause schema and require an update, as the command's
   * validation does. The filter may be left out or null.
   */
  @ParameterizedTest
  @ValueSource(strings = {"updateOne", "updateMany", "findOneAndUpdate"})
  void updateToolsRequireAnUpdateClause(String tool) throws Exception {
    var schema = inputSchema(tool);
    var payload = toolInput("{}");
    payload.putObject("update");
    assertSchemaAccepts(schema, payload);
    assertNotNull(parseUpdateCommand(tool, payload).updateClause().buildOperations());
    payload.putNull("filter");
    assertSchemaAccepts(schema, payload);
    assertNotNull(parseUpdateCommand(tool, payload).updateClause().buildOperations());

    // Any object schema accepts this. Only the update clause schema rejects it.
    payload.putObject("update").put("name", "Ada");
    assertSchemaRejects(schema, payload);
    payload.remove("update");
    assertSchemaRejects(schema, payload);
    assertFalse(validator.validate(command(tool, payload)).isEmpty());
    payload.putNull("update");
    assertSchemaRejects(schema, payload);
    assertFalse(validator.validate(command(tool, payload)).isEmpty());
  }

  // The findAndRerank rerank override has its own schema, written to match CollectionRerankDef.

  /**
   * Schema acceptance must agree with service validation of the override fields. Whether
   * credentials are allowed depends on the provider's configuration, which the schema cannot see,
   * so the test provider here accepts a shared secret.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          {"provider": "nvidia", "modelName": "nvidia/rerank-v1"}                                                             | true
          {"provider": "nvidia", "modelName": "nvidia/rerank-v1", "authentication": {}, "parameters": {}}                     | true
          {"provider": "nvidia", "modelName": "nvidia/rerank-v1", "authentication": null, "parameters": null}                 | true
          {"provider": "nvidia", "modelName": "nvidia/rerank-v1", "authentication": {"providerKey": "credential-reference"}}  | true
          {"provider": "nvidia", "modelName": "nvidia/rerank-v1", "parameters": {"truncate": "END"}}                          | false
          {}                                                                                                                  | false
          {"provider": "nvidia"}                                                                                              | false
          {"modelName": "nvidia/rerank-v1"}                                                                                   | false
          {"provider": null, "modelName": "nvidia/rerank-v1"}                                                                 | false
          {"provider": "nvidia", "modelName": null}                                                                           | false
          {"provider": null, "modelName": null}                                                                               | false
          """)
  void rerankOverrideSchemaMatchesServiceValidation(String json, boolean accepted)
      throws Exception {
    var payload = objectMapper.readTree(json);
    var command = toolInput("{}");
    command.putObject("options").set("rerank", payload);
    var errors = schemaErrors(inputSchema("findAndRerank"), command);
    assertEquals(
        accepted, errors.isEmpty(), () -> "Schema validation for " + json + " returned " + errors);

    Executable validate = () -> validateRerankOverride(payload);
    if (accepted) {
      assertDoesNotThrow(validate);
    } else {
      var error = assertThrows(RequestException.class, validate);
      assertEquals(RequestException.Code.INVALID_RERANK_OVERRIDE.name(), error.code);
    }
  }

  @Test
  void rerankOverrideExamplesDeserialize() throws Exception {
    var document = inputSchema("findAndRerank");
    var rerank = propertySchema(document, "options", "rerank");
    var examples = schemaMetadata(document, rerank, "examples");
    assertFalse(examples.isMissingNode());
    assertFalse(examples.isEmpty());
    for (var example : examples) {
      var command = toolInput("{}");
      command.putObject("options").set("rerank", example);
      assertSchemaAccepts(document, command);
      assertNotNull(
          objectMapper.treeToValue(
              example, CreateCollectionCommand.Options.RerankServiceDesc.class));
    }
  }

  // ---- OpenAPI command schemas, as the server publishes them for Swagger ----

  /** The HTTP API accepts this JSON, so the OpenAPI command schemas must accept it too. */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          FindCommand          | {"filter": {"age": {"$gt": 40}}, "sort": {"user.age": -1}}
          FindAndRerankCommand | {"sort": {"$hybrid": "search text"}, "options": {"hybridLimits": 50}}
          FindAndRerankCommand | {"sort": {"$hybrid": "search text"}, "options": {"hybridLimits": {"$vector": 50, "$lexical": 10}}}
          FindAndRerankCommand | {"sort": {"$hybrid": "search text"}, "options": {"hybridLimits": null}}
          FindAndRerankCommand | {"filter": null, "sort": null, "options": {"rerank": null}}
          UpdateOneCommand     | {"filter": {"_id": "doc1"}, "update": {"$set": {"name": "Ada"}}}
          """)
  void openApiCommandsAcceptValidPayloads(String command, String json) throws Exception {
    assertSchemaAccepts(openApiCommandSchema(command), objectMapper.readTree(json));
  }

  /**
   * A command schema that accepts everything passes the test above, so check it rejects too. Each
   * payload is an accepted one with a single clause broken.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          FindCommand          | {"filter": 5, "sort": {"user.age": -1}}
          FindCommand          | {"filter": {"age": {"$gt": 40}}, "sort": []}
          FindAndRerankCommand | {"sort": {"vectorizeSort": "query"}}
          FindAndRerankCommand | {"sort": {"$hybrid": "search text"}, "options": {"hybridLimits": {"$vector": 50}}}
          FindAndRerankCommand | {"sort": {"$hybrid": "search text"}, "options": {"rerank": {"provider": "nvidia"}}}
          UpdateOneCommand     | {"filter": {"_id": "doc1"}, "update": {"$unknown": {}}}
          """)
  void openApiCommandsRejectInvalidPayloads(String command, String json) throws Exception {
    assertSchemaRejects(openApiCommandSchema(command), objectMapper.readTree(json));
  }

  // ---- Helpers ----

  /** Generates a schema for one type the way the MCP server does for a tool argument. */
  private JsonNode schemaFor(Class<?> type) {
    var builder =
        new SchemaGeneratorConfigBuilder(
                objectMapper, SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON)
            .without(Option.SCHEMA_VERSION_INDICATOR);
    new McpSchemaCustomizer().customize(builder);
    var schema = new SchemaGenerator(builder.build()).generateSchema(type);
    McpOpenApiSchemaAdapter.bundleOpenApiSchemas(
        schema, McpOpenApiSchemaAdapter.readGeneratedOpenApiDocument());
    return schema;
  }

  /** The input schema the MCP server publishes for a registered tool. */
  private JsonNode inputSchema(String name) throws Exception {
    var tool = toolManager.getTool(name);
    assertNotNull(tool);
    return objectMapper.readTree(schemaGenerator.generate(tool).asJson());
  }

  /** A self-contained schema for one command component of the served OpenAPI document. */
  private JsonNode openApiCommandSchema(String command) throws Exception {
    var components = openApi().path("components");
    assertFalse(
        components.path("schemas").path(command).isMissingNode(),
        "Missing OpenAPI component " + command);
    var schema = objectMapper.createObjectNode().put("$ref", "#/components/schemas/" + command);
    schema.set("components", components);
    return schema;
  }

  private JsonNode openApi() throws Exception {
    var json =
        given()
            .queryParam("format", "json")
            .get("/api/json/openapi")
            .then()
            .statusCode(200)
            .extract()
            .asString();
    return objectMapper.readTree(json);
  }

  private ObjectNode toolInput(String json) throws Exception {
    // Routing arguments are required by MCP but are not part of the command clauses.
    return ((ObjectNode) objectMapper.readTree(json))
        .put("keyspace", "default_keyspace")
        .put("collection", "documents");
  }

  private Updatable parseUpdateCommand(String tool, ObjectNode payload) throws Exception {
    var command = command(tool, payload);
    assertTrue(validator.validate(command).isEmpty());
    return (Updatable) command;
  }

  /** Builds the command a tool call runs, without the MCP routing arguments. */
  private Command command(String tool, ObjectNode payload) throws Exception {
    var arguments = payload.deepCopy();
    arguments.remove(List.of("keyspace", "collection"));
    return objectMapper.treeToValue(
        objectMapper.createObjectNode().set(tool, arguments), Command.class);
  }

  private CollectionRerankDef.RerankServiceDef validateRerankOverride(JsonNode payload)
      throws Exception {
    var override =
        objectMapper.treeToValue(payload, CreateCollectionCommand.Options.RerankServiceDesc.class);
    return CollectionRerankDef.validateServiceDesc(
        sharedSecretRerankingProvider(),
        override.provider(),
        override.modelName(),
        override.authentication(),
        override.parameters(),
        RequestException.Code.INVALID_RERANK_OVERRIDE);
  }

  /** One enabled nvidia provider and model that accepts a shared secret. */
  private static RerankingProvidersConfig sharedSecretRerankingProvider() {
    var model =
        new ModelConfigImpl(
            "nvidia/rerank-v1",
            new ApiModelSupport.ApiModelSupportImpl(
                ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
            false,
            "https://example.com/rerank",
            new RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10));
    return new RerankingProvidersConfigImpl(
        Map.of(
            "nvidia",
            new RerankingProviderConfigImpl(
                false,
                "nvidia",
                true,
                Map.of(
                    AuthenticationType.SHARED_SECRET,
                    new AuthenticationConfigImpl(true, List.of())),
                List.of(model))));
  }

  /** Follows local {@code $ref} links until it reaches the schema they point to. */
  private JsonNode referencedSchema(JsonNode document, JsonNode schema) {
    var visited = new HashSet<String>();
    while (schema.has("$ref")) {
      var reference = schema.path("$ref").asText();
      assertTrue(reference.startsWith("#/"), "Expected a local schema reference");
      assertTrue(visited.add(reference), "Circular schema reference");
      schema = document.at(reference.substring(1));
      assertFalse(schema.isMissingNode(), "Unresolved schema reference " + reference);
    }
    return schema;
  }

  /** The schema of a nested property, such as {@code options} then {@code rerank}. */
  private JsonNode propertySchema(JsonNode document, String... names) {
    var schema = document;
    for (var name : names) {
      schema = referencedSchema(document, schema).path("properties").path(name);
      assertFalse(schema.isMissingNode(), "Missing property " + name);
    }
    return schema;
  }

  /** A keyword such as {@code examples}, found on the schema or through its refs and allOf. */
  private JsonNode schemaMetadata(JsonNode document, JsonNode schema, String keyword) {
    if (schema.has(keyword)) {
      return schema.get(keyword);
    }
    if (schema.has("$ref")) {
      return schemaMetadata(document, referencedSchema(document, schema), keyword);
    }
    for (var branch : schema.path("allOf")) {
      var metadata = schemaMetadata(document, branch, keyword);
      if (!metadata.isMissingNode()) {
        return metadata;
      }
    }
    return MissingNode.getInstance();
  }

  private Set<ValidationMessage> schemaErrors(JsonNode schema, JsonNode payload) {
    return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
        .getSchema(schema)
        .validate(payload);
  }

  private void assertSchemaAccepts(JsonNode schema, JsonNode payload) {
    var errors = schemaErrors(schema, payload);
    assertTrue(errors.isEmpty(), () -> "Schema rejected " + payload + ": " + errors);
  }

  private void assertSchemaRejects(JsonNode schema, JsonNode payload) {
    assertFalse(
        schemaErrors(schema, payload).isEmpty(), () -> "Schema accepted invalid " + payload);
  }
}
