package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.*;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.quarkiverse.mcp.server.GlobalInputSchemaGenerator;
import io.quarkiverse.mcp.server.ToolManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.IndexReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks that MCP and OpenAPI advertise the JSON accepted by Data API clause deserializers.
 * Reflection alone exposes internal Java fields for clauses with custom deserializers, so those
 * clauses need schema overrides.
 *
 * <p>Schema validation checks the advertised JSON shape. Deserialization and clause building check
 * that the server accepts the examples. Generated tool schemas and the OpenAPI document also verify
 * that the overrides reach clients. These checks do not execute commands or require a database.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpClauseSchemaContractTest {
  @Inject ObjectMapper objectMapper;
  @Inject GlobalInputSchemaGenerator schemaGenerator;
  @Inject ToolManager toolManager;

  record WireNames(
      @JsonProperty("wireName") String internalName, @JsonIgnore CommandFeatures commandFeatures) {}

  record PropertyNameDefaults(
      @JsonProperty("") String defaultName, @JsonProperty(" ") String whitespaceName) {}

  record IgnoredFields(
      @JsonIgnore String internal,
      @JsonIgnore(false) String visible,
      CommandFeatures unannotatedFeatures) {}

  static class GetterIgnoredField {
    public String internal;

    @JsonIgnore
    public String getInternal() {
      return internal;
    }
  }

  @Test
  void jsonIgnoreControlsFieldVisibility() {
    var properties = schemaFor(IgnoredFields.class).path("properties");
    assertFalse(properties.has("internal"));
    assertTrue(properties.has("visible"));
    assertTrue(properties.has("unannotatedFeatures"));
  }

  @Test
  void jsonIgnoreOnGetterHidesField() {
    assertFalse(schemaFor(GetterIgnoredField.class).path("properties").has("internal"));
  }

  @Test
  void fieldsUseJsonPropertyNamesAndHideCommandFeatures() {
    var properties = schemaFor(WireNames.class).path("properties");
    assertTrue(properties.has("wireName"));
    assertFalse(properties.has("internalName"));
    assertFalse(properties.has("commandFeatures"));
  }

  @Test
  void emptyAndWhitespacePropertyNamesMatchJackson() throws Exception {
    var payload =
        objectMapper.readTree(
            """
            {"defaultName": "default", " ": "explicit"}
            """);
    var schema = schemaFor(PropertyNameDefaults.class);
    var properties = schema.path("properties");
    assertEquals(payload.size(), properties.size());
    payload.fieldNames().forEachRemaining(name -> assertTrue(properties.has(name)));
    assertSchemaAccepts(schema, payload);
    assertEquals(
        new PropertyNameDefaults("default", "explicit"),
        objectMapper.treeToValue(payload, PropertyNameDefaults.class));
  }

  /** An example must satisfy both its advertised schema and the server's clause parser. */
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
      // These two clauses defer parsing until build, so deserialization alone cannot validate them.
      if (clause instanceof FilterDefinition filter) {
        assertNotNull(filter.build(new TestConstants().collectionContext()));
      } else if (clause instanceof SortDefinition sort) {
        assertNotNull(sort.build(new TestConstants().collectionContext()));
      }
    }
  }

  static Stream<Class<?>> clauseTypes() throws Exception {
    var loader = FilterDefinition.class.getClassLoader();
    // Use the build's class index so new annotated clauses join these tests automatically.
    try (var indexStream = loader.getResourceAsStream("META-INF/jandex.idx")) {
      assertNotNull(indexStream, "The application class index must be available");
      var index = new IndexReader(indexStream).read();
      var clauseTypes = new ArrayList<Class<?>>();
      for (var annotation : index.getAnnotations(Schema.class)) {
        if (annotation.target().kind() != AnnotationTarget.Kind.CLASS) {
          continue;
        }
        var type = Class.forName(annotation.target().asClass().name().toString(), false, loader);
        var schema = type.getAnnotation(Schema.class);
        // Select parsed clause types, excluding helper classes for nested JSON shapes.
        if (!schema.name().isBlank()
            && (JsonDefinition.class.isAssignableFrom(type)
                || type.isAnnotationPresent(JsonDeserialize.class))) {
          clauseTypes.add(type);
        }
      }
      assertFalse(clauseTypes.isEmpty(), "The application index must contain clause schemas");
      return clauseTypes.stream().sorted(Comparator.comparing(Class::getName));
    }
  }

  /** Both entry points accept the same clause JSON and must expose the same schema definition. */
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

  @Test
  void filterSchemaAcceptsDocumentObjectsAndNull() throws Exception {
    var annotation = FilterDefinition.class.getAnnotation(Schema.class);
    var schema = schemaFor(FilterDefinition.class);
    var component = referencedSchema(schema, schema);

    assertEquals(annotation.description(), schemaMetadata(schema, schema, "description").asText());
    assertEquals(annotation.examples().length, component.path("examples").size());
    for (var i = 0; i < annotation.examples().length; i++) {
      assertEquals(
          objectMapper.readTree(annotation.examples()[i]), component.path("examples").get(i));
    }
    assertSchemaAccepts(schema, objectMapper.readTree("null"));
    assertSchemaAccepts(schema, objectMapper.readTree("{\"some.path\": {\"$gt\": 40}}"));
    assertFalse(
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(objectMapper.readTree("[]"))
            .isEmpty());
  }

  /** A correct reusable schema has no effect unless command fields actually reference it. */
  @Test
  void openApiCommandsReferenceSharedSchemas() throws Exception {
    var document = openApi();
    var schemas = document.path("components").path("schemas");
    var find = schemas.path("FindCommand").path("properties");
    assertEquals(schemas.path("FilterDefinition"), referencedSchema(document, find.path("filter")));
    assertEquals(schemas.path("SortDefinition"), referencedSchema(document, find.path("sort")));
    var rerank = schemas.path("FindAndRerankCommand").path("properties");
    assertEquals(
        schemas.path("FindAndRerankSort"), referencedSchema(document, rerank.path("sort")));
    var options = referencedSchema(document, rerank.path("options")).path("properties");
    assertEquals(
        schemas.path("HybridLimits"), referencedSchema(document, options.path("hybridLimits")));
    var openApiOverride =
        (ObjectNode) referencedSchema(document, options.path("rerank")).deepCopy();
    // The field description belongs to the command options, not the reusable schema component.
    openApiOverride.remove("description");
    assertEquals(schemas.path("RerankServiceOverride"), openApiOverride);
    var mcpDocument = inputSchema("findAndRerank");
    var mcpOverride =
        (ObjectNode)
            referencedSchema(mcpDocument, propertySchema(mcpDocument, "options", "rerank"))
                .deepCopy();
    ((ObjectNode) mcpOverride).remove("description");
    var expected =
        objectMapper.createObjectNode().put("$ref", FindAndRerankCommand.RerankOverrideSchema.REF);
    McpOpenApiSchemaAdapter.bundleOpenApiSchemas(expected, document);
    assertEquals(referencedSchema(expected, expected), mcpOverride);
  }

  /** Check the complete command schema because inferred parent types can restrict valid clauses. */
  @ParameterizedTest
  @ValueSource(strings = {"50", "{\"$vector\": 50, \"$lexical\": 10}", "null"})
  void openApiAcceptsHybridLimitsWireForms(String limits) throws Exception {
    var document = openApi();
    var schema =
        objectMapper.createObjectNode().put("$ref", "#/components/schemas/FindAndRerankCommand");
    schema.set("components", document.path("components"));
    var command = objectMapper.createObjectNode();
    command.putObject("sort").put("$hybrid", "search text");
    command.putObject("options").set("hybridLimits", objectMapper.readTree(limits));
    assertSchemaAccepts(schema, command);
  }

  @Test
  void openApiAcceptsNullOptionalClauses() throws Exception {
    var schema =
        objectMapper.createObjectNode().put("$ref", "#/components/schemas/FindAndRerankCommand");
    schema.set("components", openApi().path("components"));
    assertSchemaAccepts(
        schema,
        objectMapper.readTree(
            """
        {"filter": null, "sort": null, "options": {"rerank": null}}
        """));
  }

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

  private JsonNode propertySchema(JsonNode document, String... names) {
    var schema = document;
    for (var name : names) {
      schema = referencedSchema(document, schema).path("properties").path(name);
      assertFalse(schema.isMissingNode(), "Missing property " + name);
    }
    return schema;
  }

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

  @Test
  void rerankToolUsesSortClassDescription() throws Exception {
    var description = FindAndRerankSort.class.getAnnotation(Schema.class).description();
    assertFalse(description.isBlank());
    var document = inputSchema("findAndRerank");
    assertEquals(
        description,
        schemaMetadata(document, propertySchema(document, "sort"), "description").asText());
  }

  /** Clause arguments take their descriptions from their classes, which Swagger also uses. */
  @Test
  void clauseArgumentsUseClassDescriptions() throws Exception {
    var filter = FilterDefinition.class.getAnnotation(Schema.class).description();
    var sort = SortDefinition.class.getAnnotation(Schema.class).description();
    var checked = 0;
    for (var tool : toolManager) {
      var document = inputSchema(tool.name());
      var properties = document.path("properties");
      if (properties.has("filter")) {
        assertEquals(
            filter,
            schemaMetadata(document, properties.path("filter"), "description").asText(),
            tool.name());
        checked++;
      }
      // findAndRerank uses FindAndRerankSort, checked in rerankToolUsesSortClassDescription.
      if (properties.has("sort") && !tool.name().equals("findAndRerank")) {
        assertEquals(
            sort,
            schemaMetadata(document, properties.path("sort"), "description").asText(),
            tool.name());
        checked++;
      }
    }
    assertTrue(checked > 0, "No tool has a filter or sort argument");
  }

  /**
   * A description that only repeats the argument name tells a client nothing. The schema already
   * lists required arguments, so descriptions do not start with "Optional" or "Required".
   */
  @Test
  void everyToolDescribesEveryArgument() throws Exception {
    for (var tool : toolManager) {
      var document = inputSchema(tool.name());
      for (var argument : document.path("properties").properties()) {
        var label = tool.name() + "." + argument.getKey();
        var description = schemaMetadata(document, argument.getValue(), "description").asText();
        assertFalse(description.isBlank(), label);
        assertNotEquals(argument.getKey(), description.toLowerCase(), label);
        assertFalse(description.matches("(?i)(optional|required)\\b.*"), label);
      }
    }
  }

  @Test
  void rerankToolUsesOverrideFieldDescription() throws Exception {
    var description =
        FindAndRerankCommand.Options.class
            .getDeclaredField("rerankServiceOverride")
            .getAnnotation(Schema.class)
            .description();
    assertFalse(description.isBlank());
    var document = inputSchema("findAndRerank");
    assertEquals(
        description,
        schemaMetadata(document, propertySchema(document, "options", "rerank"), "description")
            .asText());
  }

  /** Schema acceptance must agree with service validation of the override fields. */
  @ParameterizedTest
  @MethodSource("rerankOverrides")
  void rerankOverrideSchemaMatchesServiceValidation(String json, boolean accepted)
      throws Exception {
    var schema = inputSchema("findAndRerank");
    var payload = objectMapper.readTree(json);
    var command = toolInput("{}");
    command.putObject("options").set("rerank", payload);
    var errors =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(command);
    assertEquals(
        accepted, errors.isEmpty(), () -> "Schema validation for " + json + " returned " + errors);

    Executable validate = () -> validateRerankOverride(payload, AuthenticationType.NONE);
    if (accepted) {
      assertDoesNotThrow(validate);
    } else {
      var error = assertThrows(RequestException.class, validate);
      assertEquals(RequestException.Code.INVALID_RERANK_OVERRIDE.name(), error.code);
    }
  }

  static Stream<Arguments> rerankOverrides() {
    return Stream.of(
        Arguments.of(
            """
            {"provider": "nvidia", "modelName": "nvidia/rerank-v1"}
            """,
            true),
        Arguments.of(
            """
            {"provider": "nvidia", "modelName": "nvidia/rerank-v1",
             "authentication": {}, "parameters": {}}
            """,
            true),
        Arguments.of(
            """
            {"provider": "nvidia", "modelName": "nvidia/rerank-v1",
             "authentication": null, "parameters": null}
            """,
            true),
        Arguments.of(
            """
            {"provider": "nvidia", "modelName": "nvidia/rerank-v1",
             "parameters": {"truncate": "END"}}
            """,
            false),
        Arguments.of("{}", false),
        Arguments.of(
            """
            {"provider": "nvidia"}
            """,
            false),
        Arguments.of(
            """
            {"modelName": "nvidia/rerank-v1"}
            """,
            false),
        Arguments.of(
            """
            {"provider": null, "modelName": "nvidia/rerank-v1"}
            """,
            false),
        Arguments.of(
            """
            {"provider": "nvidia", "modelName": null}
            """,
            false),
        Arguments.of(
            """
            {"provider": null, "modelName": null}
            """,
            false));
  }

  /** The schema allows credentials because the provider configuration determines support. */
  @ParameterizedTest
  @EnumSource(AuthenticationType.class)
  void rerankAuthenticationValidationDependsOnProvider(AuthenticationType authenticationType)
      throws Exception {
    var payload =
        objectMapper.readTree(
            """
            {"provider": "nvidia", "modelName": "nvidia/rerank-v1",
             "authentication": {"providerKey": "credential-reference"}}
            """);
    var command = toolInput("{}");
    command.putObject("options").set("rerank", payload);
    assertSchemaAccepts(inputSchema("findAndRerank"), command);

    Executable validate = () -> validateRerankOverride(payload, authenticationType);
    if (authenticationType == AuthenticationType.SHARED_SECRET) {
      assertDoesNotThrow(validate);
    } else {
      var error = assertThrows(RequestException.class, validate);
      assertEquals(RequestException.Code.INVALID_RERANK_OVERRIDE.name(), error.code);
    }
  }

  private CollectionRerankDef.RerankServiceDef validateRerankOverride(
      JsonNode payload, AuthenticationType authenticationType) throws Exception {
    var override =
        objectMapper.treeToValue(payload, CreateCollectionCommand.Options.RerankServiceDesc.class);
    return CollectionRerankDef.validateServiceDesc(
        supportedRerankingProvider(authenticationType),
        override.provider(),
        override.modelName(),
        override.authentication(),
        override.parameters(),
        RequestException.Code.INVALID_RERANK_OVERRIDE);
  }

  private static RerankingProvidersConfig supportedRerankingProvider(
      AuthenticationType authenticationType) {
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
                Map.of(authenticationType, new AuthenticationConfigImpl(true, List.of())),
                List.of(model))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"50", "{\"$vector\": 50, \"$lexical\": 10}"})
  void rerankToolAcceptsWirePayloads(String limits) throws Exception {
    var payload =
        toolInput(
            """
            {
              "filter": {"age": {"$gt": 40}},
              "sort": {"$hybrid": "search text"},
              "options": {"rerank": {"provider": "nvidia", "modelName": "nvidia/rerank-v1"}}
            }
            """);
    ((ObjectNode) payload.path("options")).set("hybridLimits", objectMapper.readTree(limits));
    assertSchemaAccepts(inputSchema("findAndRerank"), payload);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"$hybrid\":{\"$vector\":null,\"$lexical\":\"query\"}}",
        "{\"$hybrid\":{\"$vectorize\":null,\"$lexical\":\"query\"}}",
        "{\"$hybrid\":{\"$vectorize\":\"query\",\"$lexical\":null}}",
        "{\"$hybrid\":{\"$vector\":[1,2,3],\"$lexical\":null}}",
        "{\"$hybrid\":{\"$vector\":null}}",
        "{\"$hybrid\":{\"$vectorize\":null}}",
        "{\"$hybrid\":{\"$lexical\":null}}",
        "{\"$hybrid\":{}}",
        "{}",
        "null"
      })
  void sortSchemaAcceptsNullableAndEmptyWireForms(String json) throws Exception {
    var sort = objectMapper.readTree(json);
    assertSchemaAccepts(schemaFor(FindAndRerankSort.class), sort);
    var clause = assertDoesNotThrow(() -> objectMapper.treeToValue(sort, FindAndRerankSort.class));
    if (!sort.isNull()) {
      assertNotNull(clause);
    }

    var payload = toolInput("{}");
    payload.set("sort", sort);
    assertSchemaAccepts(inputSchema("findAndRerank"), payload);
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

  @Test
  void findToolAcceptsWirePayloads() throws Exception {
    var payload =
        toolInput(
            """
            {"filter": {"age": {"$gt": 40}}, "sort": {"user.age": -1, "user.name": 1}}
            """);
    assertSchemaAccepts(inputSchema("find"), payload);
  }

  @ParameterizedTest
  @MethodSource("invalidToolPayloads")
  void toolSchemasRejectInvalidPayloads(String tool, String json) throws Exception {
    var schema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(inputSchema(tool));
    assertFalse(schema.validate(toolInput(json)).isEmpty());
  }

  static Stream<Arguments> invalidToolPayloads() {
    return Stream.of(
        Arguments.of("find", "{\"filter\": 5}"),
        Arguments.of("find", "{\"sort\": []}"),
        Arguments.of("findAndRerank", "{\"sort\": {\"vectorizeSort\": \"query\"}}"),
        Arguments.of("findAndRerank", "{\"sort\": {\"$hybrid\": {\"$vector\": \"query\"}}}"),
        Arguments.of("findAndRerank", "{\"options\": {\"hybridLimits\": {\"$vector\": 50}}}"),
        Arguments.of("findAndRerank", "{\"options\": {\"rerank\": {\"provider\": \"nvidia\"}}}"));
  }

  private ObjectNode toolInput(String json) throws Exception {
    // Routing arguments are required by MCP but are not part of the command clauses.
    return ((ObjectNode) objectMapper.readTree(json))
        .put("keyspace", "default_keyspace")
        .put("collection", "documents");
  }

  /** Positive examples alone would also pass against a schema that accepts every JSON value. */
  @ParameterizedTest
  @MethodSource("invalidWireShapes")
  void schemaRejectsInvalidWireShapes(Class<?> clauseType, String json) throws Exception {
    var schema =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schemaFor(clauseType));
    assertFalse(schema.validate(objectMapper.readTree(json)).isEmpty());
  }

  static Stream<Arguments> invalidWireShapes() {
    return Stream.of(
        Arguments.of(FindAndRerankSort.class, "{\"vectorizeSort\": \"query\"}"),
        Arguments.of(FindAndRerankSort.class, "{\"$hybrid\": {\"$lexical\": 1}}"),
        Arguments.of(FindAndRerankSort.class, "{\"$hybrid\": {\"$vector\": \"query\"}}"),
        Arguments.of(
            FindAndRerankSort.class, "{\"$hybrid\": {\"$vector\": null, \"$vectorize\": null}}"),
        Arguments.of(
            FindAndRerankCommand.HybridLimits.class, "{\"vectorLimit\": 50, \"lexicalLimit\": 10}"),
        Arguments.of(FindAndRerankCommand.HybridLimits.class, "{\"$vector\": 50}"),
        Arguments.of(
            FindAndRerankCommand.HybridLimits.class, "{\"$vector\": 50, \"$lexical\": \"10\"}"));
  }

  private void assertSchemaAccepts(JsonNode schema, JsonNode example) {
    var validator =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(schema);
    assertTrue(validator.validate(example).isEmpty(), () -> validator.validate(example).toString());
  }

  @Test
  void collectionCreationAcceptsRerankAuthenticationAndParameters() throws Exception {
    var payload =
        objectMapper.readTree(
            """
            {
              "provider": "nvidia", "modelName": "nvidia/rerank-v1",
              "authentication": {"providerKey": "credential-reference"},
              "parameters": {"truncate": "END"}
            }
            """);
    assertSchemaAccepts(
        schemaFor(CreateCollectionCommand.Options.RerankServiceDesc.class), payload);
    assertEquals(
        new CreateCollectionCommand.Options.RerankServiceDesc(
            "nvidia",
            "nvidia/rerank-v1",
            Map.of("providerKey", "credential-reference"),
            Map.of("truncate", "END")),
        objectMapper.treeToValue(payload, CreateCollectionCommand.Options.RerankServiceDesc.class));
  }

  @Test
  void allRegisteredToolsGenerateInputSchemas() {
    var count = 0;
    // Exercise named OpenAPI lookups for every tool, including ordinary options records.
    for (var tool : toolManager) {
      var schema =
          assertDoesNotThrow(
              () -> objectMapper.readTree(schemaGenerator.generate(tool).asJson()), tool.name());
      assertTrue(schema.path("properties").isObject(), tool.name());
      count++;
    }
    assertTrue(count > 0, "The MCP server must register tools");
  }

  private JsonNode inputSchema(String name) throws Exception {
    // The registered tool includes nested command fields and the application's customizer wiring.
    var tool = toolManager.getTool(name);
    assertNotNull(tool);
    return objectMapper.readTree(schemaGenerator.generate(tool).asJson());
  }

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
}
