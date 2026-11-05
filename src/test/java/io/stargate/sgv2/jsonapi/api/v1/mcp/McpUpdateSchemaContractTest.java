package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.quarkiverse.mcp.server.GlobalInputSchemaGenerator;
import io.quarkiverse.mcp.server.ToolManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Checks update arguments advertised by the registered tools against the real command parser. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpUpdateSchemaContractTest {
  private static final List<String> TOOLS = List.of("updateOne", "updateMany", "findOneAndUpdate");

  @Inject ObjectMapper objectMapper;
  @Inject GlobalInputSchemaGenerator schemaGenerator;
  @Inject ToolManager toolManager;
  @Inject Validator validator;

  @ParameterizedTest
  @ValueSource(strings = {"updateOne", "updateMany", "findOneAndUpdate"})
  void advertisedExamplesValidateAndBuild(String tool) throws Exception {
    var schema = inputSchema(tool);
    var examples = updateSchema(schema).path("examples");
    assertFalse(examples.isEmpty(), "Update arguments must advertise usable examples");
    for (var update : examples) {
      var payload = toolInput(update);
      assertSchemaAccepts(schema, payload);
      assertNotNull(parseCommand(tool, payload).updateClause().buildOperations());
    }
  }

  @ParameterizedTest
  @MethodSource("supportedUpdates")
  void toolSchemasAcceptSupportedUpdates(String tool, String json) throws Exception {
    var payload = toolInput(objectMapper.readTree(json));
    assertSchemaAccepts(inputSchema(tool), payload);
    assertNotNull(parseCommand(tool, payload).updateClause().buildOperations());
  }

  static Stream<Arguments> supportedUpdates() {
    // Nonempty arguments exercise each operation's parser, including supported array modifiers.
    var updates =
        List.of(
            "{}",
            "{\"$set\": {}}",
            "{\"$set\": {\"profile.name\": \"Ada\", \"optional\": null}}",
            "{\"$unset\": {\"obsolete\": null}}",
            "{\"$setOnInsert\": {\"created\": true}}",
            "{\"$inc\": {\"score\": 0.25}}",
            "{\"$mul\": {\"score\": 1.5}}",
            "{\"$min\": {\"score\": 2.5}}",
            "{\"$max\": {\"score\": 9.5}}",
            "{\"$rename\": {\"oldName\": \"newName\"}}",
            "{\"$currentDate\": {\"updated\": true, \"reviewed\": {\"$type\": \"date\"}}}",
            "{\"$pop\": {\"first\": -1, \"last\": 1}}",
            "{\"$push\": {\"items\": {\"$each\": [1, 2], \"$position\": 0}}}",
            "{\"$addToSet\": {\"tags\": {\"$each\": [\"a\", \"b\"]}}}",
            """
            {"$set": {"created": {"$date": 123456},
              "owner": {"$uuid": "550e8400-e29b-41d4-a716-446655440000"},
              "nested": {"values": [null, true, 1.25, {"name": "Ada"}]}},
             "$unset": {"obsolete": 1}}
            """);
    return TOOLS.stream().flatMap(tool -> updates.stream().map(json -> Arguments.of(tool, json)));
  }

  @ParameterizedTest
  @MethodSource("invalidUpdates")
  void toolSchemasRejectMalformedUpdates(String tool, String json) throws Exception {
    var update = objectMapper.readTree(json);
    assertSchemaRejects(inputSchema(tool), toolInput(update));
    assertThrows(UpdateException.class, () -> objectMapper.treeToValue(update, UpdateClause.class));
  }

  static Stream<Arguments> invalidUpdates() {
    var updates =
        List.of(
            "[]",
            "5",
            "\"update\"",
            "{\"name\": \"Ada\"}",
            "{\"updateOperationDefs\": {}}",
            "{\"$unknown\": {}}",
            "{\"$set\": null}",
            "{\"$set\": 5}",
            "{\"$set\": []}",
            "{\"$set\": \"value\"}");
    return TOOLS.stream().flatMap(tool -> updates.stream().map(json -> Arguments.of(tool, json)));
  }

  // updateOne is the only update tool that also serves tables.
  @Test
  void updateOneAcceptsTableOnlyPullAll() throws Exception {
    var payload = toolInput(objectMapper.readTree("{\"$pullAll\": {\"listColumn\": [1]}}"));
    assertSchemaAccepts(inputSchema("updateOne"), payload);
    assertEquals(
        Set.of(UpdateOperator.PULL_ALL),
        parseCommand("updateOne", payload).updateClause().updateOperationDefs().keySet());
  }

  @ParameterizedTest
  @ValueSource(strings = {"updateOne", "updateMany", "findOneAndUpdate"})
  void updateIsRequiredAndFilterMayBeOmittedOrNull(String tool) throws Exception {
    var schema = inputSchema(tool);
    var payload = toolInput(objectMapper.readTree("{}"));
    assertSchemaAccepts(schema, payload);
    assertNotNull(parseCommand(tool, payload).updateClause().buildOperations());
    payload.putNull("filter");
    assertSchemaAccepts(schema, payload);
    assertNotNull(parseCommand(tool, payload).updateClause().buildOperations());

    payload.remove("update");
    assertSchemaRejects(schema, payload);
    assertFalse(validator.validate(command(tool, payload)).isEmpty());
    payload.putNull("update");
    assertSchemaRejects(schema, payload);
    assertFalse(validator.validate(command(tool, payload)).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"updateOne", "updateMany", "findOneAndUpdate"})
  void openApiCommandsAcceptAdvertisedUpdateExamples(String tool) throws Exception {
    var document =
        objectMapper.readTree(
            given()
                .queryParam("format", "json")
                .get("/api/json/openapi")
                .then()
                .statusCode(200)
                .extract()
                .asString());
    var name = Character.toUpperCase(tool.charAt(0)) + tool.substring(1) + "Command";
    var schema = objectMapper.createObjectNode().put("$ref", "#/components/schemas/" + name);
    schema.set("components", document.path("components"));
    var examples = updateSchema(inputSchema(tool)).path("examples");
    assertFalse(examples.isEmpty(), "OpenAPI checks must exercise the advertised update examples");
    for (var update : examples) {
      var payload = objectMapper.createObjectNode().set("update", update);
      assertSchemaAccepts(schema, payload);
    }
  }

  // $pullAll is table-only, and every advertised example must also build for collections.
  @ParameterizedTest
  @EnumSource(value = UpdateOperator.class, names = "PULL_ALL", mode = EnumSource.Mode.EXCLUDE)
  void everySupportedOperatorHasAnAdvertisedExample(UpdateOperator operator) throws Exception {
    var examples = updateSchema(inputSchema("updateOne")).path("examples");
    assertTrue(
        StreamSupport.stream(examples.spliterator(), false)
            .anyMatch(example -> example.has(operator.apiName())),
        () -> "Missing example for " + operator.apiName());
  }

  private Updatable parseCommand(String tool, ObjectNode payload) throws Exception {
    var command = command(tool, payload);
    assertTrue(validator.validate(command).isEmpty());
    return (Updatable) command;
  }

  private Command command(String tool, ObjectNode payload) throws Exception {
    var arguments = payload.deepCopy();
    arguments.remove(List.of("keyspace", "collection"));
    return objectMapper.treeToValue(
        objectMapper.createObjectNode().set(tool, arguments), Command.class);
  }

  private ObjectNode toolInput(JsonNode update) {
    var payload = objectMapper.createObjectNode();
    payload.put("keyspace", "default_keyspace").put("collection", "documents");
    payload.set("update", update);
    return payload;
  }

  private JsonNode inputSchema(String tool) throws Exception {
    return objectMapper.readTree(schemaGenerator.generate(toolManager.getTool(tool)).asJson());
  }

  private static JsonNode updateSchema(JsonNode document) {
    var schema = document.path("properties").path("update");
    var visited = new HashSet<String>();
    while (schema.has("$ref")) {
      var reference = schema.path("$ref").asText();
      assertTrue(reference.startsWith("#/"), reference);
      assertTrue(visited.add(reference), "Circular update schema reference");
      schema = document.at(reference.substring(1));
      assertFalse(schema.isMissingNode(), "Unresolved update schema " + reference);
    }
    return schema;
  }

  private void assertSchemaAccepts(JsonNode schema, JsonNode payload) {
    var errors =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(payload);
    assertTrue(errors.isEmpty(), () -> "Schema validation for " + payload + " returned " + errors);
  }

  private void assertSchemaRejects(JsonNode schema, JsonNode payload) {
    assertFalse(
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(payload)
            .isEmpty(),
        () -> "Schema accepted malformed update " + payload);
  }
}
