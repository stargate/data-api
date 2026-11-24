package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Replays the vectorize-base Testbench suite over MCP using the local test embedding provider. */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class VectorizeBaseMcpIntegrationTest extends McpIntegrationTestBase {
  // Original suite from stargate/data-api at 4e6a2dd4c11de5cb261db6f0327d68fbe2125877.
  private static final String SUITE = "/testbench/testsuites/vectorize-base.json";

  private boolean keyspaceCreated;

  @Override
  protected MultiMap authHeaders() {
    return super.authHeaders()
        .add(
            HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
            CustomITEmbeddingProvider.TEST_API_KEY);
  }

  @BeforeAll
  void createVectorizeCollection() {
    createKeyspace(keyspaceName);
    keyspaceCreated = true;
    createCollection(
        keyspaceName,
        collectionName,
        Map.of(
            "vector",
            Map.of(
                "dimension",
                5,
                "metric",
                "cosine",
                "service",
                Map.of("provider", "custom", "modelName", "text-embedding-ada-002"))));
  }

  @AfterAll
  void removeTestKeyspace() {
    if (keyspaceCreated) {
      assertAll(
          () ->
              callToolAndAssert(
                  CommandName.Names.DELETE_COLLECTION,
                  Map.of("keyspace", keyspaceName, "collection", collectionName),
                  assertStatusOnlyOk()),
          () -> dropKeyspace(keyspaceName));
    }
  }

  @Test
  void replaysVectorizeBaseWithoutVectorizeSorts() throws IOException {
    JsonObject suite;
    try (var input = getClass().getResourceAsStream(SUITE)) {
      assertThat(input).as("Testbench suite %s", SUITE).isNotNull();
      suite = new JsonObject(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }

    var insertedIds = new ArrayList<String>();
    for (var command : objects(suite.getJsonArray("setup"))) {
      var tool = toolName(command);
      var arguments = command.getJsonObject(tool);
      var expectedIds =
          switch (tool) {
            case "insertOne" -> List.of(arguments.getJsonObject("document").getString("_id"));
            case "insertMany" -> ids(arguments.getJsonArray("documents"));
            default -> throw new AssertionError("Unexpected setup command: " + tool);
          };
      replay(
          command,
          assertStatusOnlyWithJson(
              status ->
                  assertThat(status.getJsonArray("insertedIds").stream())
                      .containsExactlyInAnyOrderElementsOf(expectedIds)));
      insertedIds.addAll(expectedIds);
    }

    for (var test : objects(suite.getJsonArray("tests"))) {
      var command = test.getJsonObject("command");
      var asserts = test.getJsonObject("asserts");
      var tool = toolName(command);
      switch (tool) {
        case "find" ->
            replay(
                command,
                assertDataOnly(
                    data ->
                        assertThat(ids(data.getJsonArray("documents")))
                            .hasSize(asserts.getInteger("Documents.count"))
                            .containsExactlyInAnyOrderElementsOf(insertedIds)));
        case "findOneAndUpdate" -> {
          var set = command.getJsonObject(tool).getJsonObject("update").getJsonObject("$set");
          replay(
              command,
              assertDataAndStatus(
                  data -> {
                    var document = data.getJsonObject("document");
                    assertThat(document).isNotNull();
                    // With no sort, any of the inserted documents can be selected.
                    assertThat(document.getString("_id")).isIn(insertedIds);
                    for (var field : set.fieldNames()) {
                      assertThat(document.getValue(field)).as(field).isEqualTo(set.getValue(field));
                    }
                  },
                  status ->
                      assertThat(status).isEqualTo(asserts.getJsonObject("Status.isExactly"))));
        }
        default -> throw new AssertionError("Unexpected test command: " + tool);
      }
    }
  }

  private void replay(JsonObject command, Consumer<ToolResponse> assertions) {
    var tool = toolName(command);
    var arguments = command.getJsonObject(tool).copy();
    var sort = arguments.getJsonObject("sort");
    if (sort != null && sort.containsKey("$vectorize")) {
      sort.remove("$vectorize");
      if (sort.isEmpty()) {
        arguments.remove("sort");
      }
    }
    arguments.put("keyspace", keyspaceName).put("collection", collectionName);

    callToolAndAssert(
        tool,
        arguments.getMap(),
        response -> {
          assertThat(response.isError()).as("MCP %s response: %s", tool, response).isFalse();
          assertions.accept(response);
        });
  }

  private static String toolName(JsonObject command) {
    assertThat(command.fieldNames()).as("Testbench command %s", command).hasSize(1);
    return command.fieldNames().iterator().next();
  }

  private static List<String> ids(JsonArray documents) {
    return objects(documents).stream().map(document -> document.getString("_id")).toList();
  }

  private static List<JsonObject> objects(JsonArray array) {
    return array.stream().map(JsonObject.class::cast).toList();
  }
}
