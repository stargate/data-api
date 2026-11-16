package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.quarkiverse.mcp.server.test.McpAssured.ToolsPage;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

/** Checks the schemas clients receive from the packaged application's tools/list endpoint. */
@QuarkusIntegrationTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpSchemaIntegrationTest extends McpIntegrationTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonSchemaFactory SCHEMAS =
      JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

  // Tool discovery loads the packaged OpenAPI document without executing database commands.
  @Test
  void advertisedToolSchemasResolveTheirReferences() {
    mcpClient
        .when()
        .toolsList(
            page -> {
              assertFalse(page.tools().isEmpty());
              assertNull(page.nextCursor(), "Validate every advertised tool in this response");
              assertNotNull(page.findByName("find"));
              assertNotNull(page.findByName("findAndRerank"));
              for (var tool : page.tools()) {
                assertEquals("object", tool.inputSchema().getString("type"), tool.name());
                assertDoesNotThrow(
                    () -> schemaFor(page, tool.name()).initializeValidators(), tool.name());
              }
            })
        .thenAssertResults();
  }

  @Test
  void findAdvertisesDocumentPathsInsteadOfJavaFields() {
    mcpClient
        .when()
        .toolsList(
            page -> {
              var schema = schemaFor(page, "find");
              var arguments =
                  new JsonObject(
                      """
                      {
                        "keyspace":"default_keyspace",
                        "collection":"schema_test",
                        "filter":{"empty":{"$eq":true},"json":"example"},
                        "sort":{"age":-1}
                      }
                      """);
              assertAccepts(schema, arguments);
              assertRejects(schema, arguments.copy().put("filter", "not an object"));
              assertRejects(schema, arguments.copy().put("sort", "not an object"));
            })
        .thenAssertResults();
  }

  @Test
  void createCollectionResolvesProviderOptionsDefinitions() {
    mcpClient
        .when()
        .toolsList(
            page -> {
              var schema = schemaFor(page, "createCollection");
              var arguments =
                  new JsonObject(
                      """
                      {
                        "keyspace":"default_keyspace",
                        "collection":"schema_test",
                        "options":{"rerank":{"service":{
                          "provider":"nvidia",
                          "modelName":"nvidia/rerank-v1",
                          "authentication":{"providerKey":"credential-reference"},
                          "parameters":{"truncate":"END"}
                        }}}
                      }
                      """);
              assertAccepts(schema, arguments);

              var invalidAuthentication = arguments.copy();
              invalidAuthentication
                  .getJsonObject("options")
                  .getJsonObject("rerank")
                  .getJsonObject("service")
                  .put("authentication", "not an object");
              assertRejects(schema, invalidAuthentication);
            })
        .thenAssertResults();
  }

  @Test
  void findAndRerankPreservesNestedConstraints() {
    mcpClient
        .when()
        .toolsList(
            page -> {
              var schema = schemaFor(page, "findAndRerank");
              var arguments =
                  new JsonObject(
                      """
                      {
                        "keyspace":"default_keyspace",
                        "collection":"schema_test",
                        "filter":null,
                        "sort":{"$hybrid":{"$vector":[1,2,3],"$lexical":"coffee"}},
                        "options":{"hybridLimits":{"$vector":50,"$lexical":10}}
                      }
                      """);
              assertAccepts(schema, arguments);

              var binaryVector = arguments.copy();
              binaryVector
                  .getJsonObject("sort")
                  .getJsonObject("$hybrid")
                  .put("$vector", new JsonObject().put("$binary", "P4AAAEAAAABAQAAA"));
              assertAccepts(schema, binaryVector);

              var shorthand =
                  arguments.copy().put("sort", new JsonObject().put("$hybrid", "coffee"));
              shorthand.getJsonObject("options").put("hybridLimits", 50);
              assertAccepts(schema, shorthand);

              var conflictingVector = arguments.copy();
              conflictingVector
                  .getJsonObject("sort")
                  .getJsonObject("$hybrid")
                  .put("$vectorize", "coffee");
              assertRejects(schema, conflictingVector);

              var missingLimit = arguments.copy();
              missingLimit
                  .getJsonObject("options")
                  .getJsonObject("hybridLimits")
                  .remove("$lexical");
              assertRejects(schema, missingLimit);
            })
        .thenAssertResults();
  }

  private static JsonSchema schemaFor(ToolsPage page, String name) {
    var tool = page.findByName(name);
    assertNotNull(tool, name);
    return SCHEMAS.getSchema(json(tool.inputSchema()));
  }

  private static void assertAccepts(JsonSchema schema, JsonObject arguments) {
    var errors = schema.validate(json(arguments));
    assertTrue(errors.isEmpty(), errors::toString);
  }

  private static void assertRejects(JsonSchema schema, JsonObject arguments) {
    assertFalse(schema.validate(json(arguments)).isEmpty());
  }

  private static JsonNode json(JsonObject value) {
    return assertDoesNotThrow(() -> MAPPER.readTree(value.encode()));
  }
}
