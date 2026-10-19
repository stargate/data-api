package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Checks the behavior hints clients receive from the registered tools over HTTP. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class McpToolAnnotationsTest {
  private final Map<String, Map<String, Object>> tools = new HashMap<>();

  @BeforeEach
  void listTools() {
    if (!tools.isEmpty()) {
      return;
    }
    List<Map<String, Object>> registeredTools =
        given()
            .contentType(ContentType.JSON)
            .accept("application/json, text/event-stream")
            .header("Token", "annotations-test-credential")
            .header("MCP-Protocol-Version", "2026-07-28")
            .header("Mcp-Method", "tools/list")
            .body(
                Map.of(
                    "jsonrpc",
                    "2.0",
                    "id",
                    1,
                    "method",
                    "tools/list",
                    "params",
                    Map.of(
                        "_meta",
                        Map.of(
                            "io.modelcontextprotocol/protocolVersion",
                            "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities",
                            Map.of()))))
            .post("/v1/mcp")
            .then()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getList("result.tools");
    for (var tool : registeredTools) {
      assertThat(tools.put((String) tool.get("name"), tool)).isNull();
    }
  }

  @Test
  void listsEveryToolWithBehaviorHints() {
    assertThat(tools).hasSize(35);
    tools.forEach((name, tool) -> assertThat(tool).as(name).containsKey("annotations"));
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({
    "createKeyspace, false, false, true, false",
    "dropKeyspace, false, true, true, false",
    "findKeyspaces, true, false, true, false",
    "findEmbeddingProviders, true, false, true, false",
    "findRerankingProviders, true, false, true, false",
    "createCollection, false, false, true, false",
    "deleteCollection, false, true, true, false",
    "findCollections, true, false, true, false",
    "alterType, false, true, false, false",
    "createTable, false, false, true, false",
    "createType, false, false, true, false",
    "dropIndex, false, true, true, false",
    "dropTable, false, true, true, false",
    "dropType, false, true, true, false",
    "listTables, true, false, true, false",
    "listTypes, true, false, true, false",
    "alterTable, false, true, false, false",
    "countDocuments, true, false, true, false",
    "createIndex, false, false, true, false",
    "createTextIndex, false, false, true, false",
    "createVectorIndex, false, false, true, false",
    "deleteMany, false, true, false, false",
    "deleteOne, false, true, false, true",
    "estimatedDocumentCount, true, false, true, false",
    "findAndRerank, true, false, false, true",
    "find, true, false, false, true",
    "findOneAndDelete, false, true, false, true",
    "findOneAndReplace, false, true, false, true",
    "findOneAndUpdate, false, true, false, true",
    "findOne, true, false, false, true",
    "listIndexes, true, false, true, false",
    "insertMany, false, true, false, true",
    "insertOne, false, true, false, true",
    "updateMany, false, true, false, true",
    "updateOne, false, true, false, true"
  })
  void publishesReviewedBehaviorHints(
      String name, boolean readOnly, boolean destructive, boolean idempotent, boolean openWorld) {
    assertThat(tools).containsKey(name);
    assertThat(tools.get(name).get("annotations"))
        .as("Behavior hints for %s", name)
        .isEqualTo(
            Map.of(
                "readOnlyHint", readOnly,
                "destructiveHint", destructive,
                "idempotentHint", idempotent,
                "openWorldHint", openWorld));
  }
}
