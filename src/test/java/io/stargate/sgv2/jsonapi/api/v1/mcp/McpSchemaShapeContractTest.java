package io.stargate.sgv2.jsonapi.api.v1.mcp;

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
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks MCP tool input schemas for arguments whose JSON differs from their Java type.
 *
 * <p>Table columns are a map subclass. A table primary key, the column of a regular index, and a
 * collection's lexical analyzer each accept a string or an object through a custom deserializer. A
 * schema generated from the Java fields describes JSON the server does not accept. No commands run
 * and no database is needed.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpSchemaShapeContractTest {
  @Inject ObjectMapper objectMapper;
  @Inject GlobalInputSchemaGenerator schemaGenerator;
  @Inject ToolManager toolManager;

  /** A map subclass must be described by its entries, not by the fields of java.util maps. */
  @Test
  void noToolShowsJavaMapFields() throws Exception {
    for (var tool : toolManager) {
      var document = inputSchema(tool.name());
      for (var name : new String[] {"accessOrder", "loadFactor", "threshold"}) {
        assertNull(document.findParent(name), () -> tool.name() + " schema shows " + name);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        """
        {"id": "text", "age": "int"}
        """,
        """
        {"id": "text", "embedding": {"type": "vector", "dimension": 3}}
        """
      })
  void createTableAcceptsColumnsTheServerParses(String columns) throws Exception {
    var definition = objectMapper.createObjectNode().put("primaryKey", "id");
    definition.set("columns", objectMapper.readTree(columns));
    var input = toolInput("readers").<ObjectNode>set("definition", definition);

    assertDoesNotThrow(() -> command("createTable", input));
    assertSchemaAccepts(inputSchema("createTable"), input);
  }

  @Test
  void createTableRejectsColumnsThatAreNotAnObject() throws Exception {
    var definition = objectMapper.createObjectNode().put("primaryKey", "id").put("columns", "id");
    assertSchemaRejects(
        inputSchema("createTable"), toolInput("readers").set("definition", definition));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        """
        "age"
        """,
        """
        {"tags": "$keys"}
        """,
        """
        {"tags": "$values"}
        """
      })
  void createIndexAcceptsColumnsTheServerParses(String column) throws Exception {
    var definition = objectMapper.createObjectNode().set("column", objectMapper.readTree(column));
    var input = toolInput("readers").put("indexName", "readers_idx");
    input.set("definition", definition);

    assertDoesNotThrow(() -> command("createIndex", input));
    assertSchemaAccepts(inputSchema("createIndex"), input);
  }

  @ParameterizedTest
  @ValueSource(strings = {"5", "[\"age\"]", "true"})
  void createIndexRejectsColumnsTheServerRejects(String column) throws Exception {
    var definition = objectMapper.createObjectNode().set("column", objectMapper.readTree(column));
    var input = toolInput("readers").put("indexName", "readers_idx");
    input.set("definition", definition);

    assertThrows(Exception.class, () -> command("createIndex", input));
    assertSchemaRejects(inputSchema("createIndex"), input);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        """
        "standard"
        """,
        """
        {"tokenizer": {"name": "whitespace"}}
        """
      })
  void createCollectionAcceptsAnalyzersTheServerParses(String analyzer) throws Exception {
    var lexical = objectMapper.createObjectNode().put("enabled", true);
    lexical.set("analyzer", objectMapper.readTree(analyzer));
    var input =
        objectMapper
            .createObjectNode()
            .put("keyspace", "default_keyspace")
            .put("collection", "documents");
    input.set("options", objectMapper.createObjectNode().set("lexical", lexical));

    var command = input.deepCopy();
    command.remove(List.of("keyspace", "collection"));
    command.put("name", "documents");
    assertDoesNotThrow(
        () ->
            objectMapper.treeToValue(
                objectMapper.createObjectNode().set("createCollection", command), Command.class));
    assertSchemaAccepts(inputSchema("createCollection"), input);
  }

  @Test
  void createCollectionRejectsAnAnalyzerThatIsNotAStringOrObject() throws Exception {
    var lexical = objectMapper.createObjectNode().put("enabled", true).put("analyzer", 5);
    var input =
        objectMapper
            .createObjectNode()
            .put("keyspace", "default_keyspace")
            .put("collection", "documents");
    input.set("options", objectMapper.createObjectNode().set("lexical", lexical));
    assertSchemaRejects(inputSchema("createCollection"), input);
  }

  /** The input schema the MCP server publishes for a registered tool. */
  private JsonNode inputSchema(String name) throws Exception {
    var tool = toolManager.getTool(name);
    assertNotNull(tool);
    return objectMapper.readTree(schemaGenerator.generate(tool).asJson());
  }

  /** Builds the command a tool call runs, without the MCP routing arguments. */
  private Command command(String tool, ObjectNode payload) throws Exception {
    var arguments = payload.deepCopy();
    arguments.remove(List.of("keyspace", "table"));
    // The createIndex tool names this argument indexName. The command JSON calls it name.
    var indexName = arguments.remove("indexName");
    if (indexName != null) {
      arguments.set("name", indexName);
    }
    return objectMapper.treeToValue(
        objectMapper.createObjectNode().set(tool, arguments), Command.class);
  }

  private ObjectNode toolInput(String table) {
    // Routing arguments are required by MCP but are not part of the command.
    return objectMapper.createObjectNode().put("keyspace", "default_keyspace").put("table", table);
  }

  private void assertSchemaAccepts(JsonNode schema, JsonNode payload) {
    var errors =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(payload);
    assertTrue(errors.isEmpty(), () -> "Schema rejected " + payload + ": " + errors);
  }

  private void assertSchemaRejects(JsonNode schema, JsonNode payload) {
    var errors =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(schema)
            .validate(payload);
    assertFalse(errors.isEmpty(), () -> "Schema accepted invalid " + payload);
  }
}
