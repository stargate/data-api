package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.completeResult;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.readableResult;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.toolPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCache;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCacheSupplier;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/** Compares command validation at the real HTTP and MCP boundaries before schema or execution. */
@QuarkusTest
@TestProfile(McpCommandValidationTest.Profile.class)
class McpCommandValidationTest {
  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final String TOKEN = "command-validation-test-credential";

  @InjectMock SchemaObjectCacheSupplier schemaObjectCacheSupplier;
  @InjectMock MeteredCommandProcessor commandProcessor;
  private SchemaObjectCache schemaObjectCache;

  @BeforeEach
  void setUp() {
    schemaObjectCache = mock(SchemaObjectCache.class);
    when(schemaObjectCacheSupplier.get()).thenReturn(schemaObjectCache);
    doReturn(Uni.createFrom().item(TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT))
        .when(schemaObjectCache)
        .getTableBased(any(), any(), any(), anyBoolean());
    when(commandProcessor.processCommand(any(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
                        .addStatus(CommandStatus.OK, 1)
                        .build()));
  }

  @ParameterizedTest
  @MethodSource("invalidCommands")
  void rejectsInvalidCommandsBeforeSchemaOrExecution(
      String command, Map<String, Object> arguments, String reason) {
    var httpResult = callHttp(command, arguments);
    assertValidationError(httpResult, reason);
    verifyNoInteractions(schemaObjectCacheSupplier, schemaObjectCache, commandProcessor);

    var mcpResult = callMcp(command, arguments);
    assertThat(mcpResult.getBoolean("isError")).isTrue();
    assertThat(readableResult(mcpResult)).isEqualTo(mcpResult.getJsonObject("structuredContent"));
    assertValidationError(readableResult(mcpResult), reason);
    verifyNoInteractions(schemaObjectCacheSupplier, schemaObjectCache, commandProcessor);
  }

  static Stream<Arguments> invalidCommands() {
    return Stream.of(
        Arguments.of(
            CommandName.Names.INSERT_MANY, Map.of("documents", List.of()), "must not be empty"),
        Arguments.of(
            CommandName.Names.INSERT_MANY,
            Map.of("documents", List.of(Map.of(), Map.of(), Map.of())),
            "amount of documents to insert is over the max limit (3 vs 2)"),
        Arguments.of(
            CommandName.Names.FIND,
            Map.of("options", Map.of("limit", 0)),
            "limit should be greater than `0`"),
        Arguments.of(
            CommandName.Names.FIND,
            Map.of("options", Map.of("skip", 1)),
            "skip options should be used with sort clause"));
  }

  @ParameterizedTest
  @MethodSource("validCommands")
  void acceptsValidAndOptionalArguments(String command, Map<String, Object> arguments) {
    var httpResult = callHttp(command, arguments);
    var mcpResult = callMcp(command, arguments);

    assertThat(httpResult.getJsonObject("status")).as(httpResult.encode()).isNotNull();
    assertThat(httpResult.getJsonObject("status").getInteger("ok")).isEqualTo(1);
    assertThat(httpResult.containsKey("errors")).isFalse();
    assertThat(mcpResult.getBoolean("isError")).isFalse();
    assertThat(readableResult(mcpResult)).isEqualTo(httpResult);
    var commands = ArgumentCaptor.forClass(Command.class);
    verify(commandProcessor, times(2)).processCommand(any(), commands.capture());
    assertThat(commands.getAllValues().get(0)).isEqualTo(commands.getAllValues().get(1));
    verify(schemaObjectCache, times(2)).getTableBased(any(), any(), any(), anyBoolean());
  }

  static Stream<Arguments> validCommands() {
    var nullOptions = new HashMap<String, Object>();
    nullOptions.put("options", null);
    return Stream.of(
        Arguments.of(
            CommandName.Names.INSERT_MANY,
            Map.of("documents", List.of(Map.of("text", "café 🐘 'quoted'"), Map.of()))),
        Arguments.of(CommandName.Names.FIND, Map.of()),
        Arguments.of(CommandName.Names.FIND, nullOptions),
        Arguments.of(CommandName.Names.FIND, Map.of("options", Map.of())),
        Arguments.of(CommandName.Names.FIND, Map.of("options", Map.of("limit", 1))));
  }

  private JsonObject callHttp(String command, Map<String, Object> arguments) {
    return new JsonObject(
        given()
            .contentType(ContentType.JSON)
            .header("Token", TOKEN)
            .body(Map.of(command, arguments))
            .post("/v1/" + TEST_CONSTANTS.KEYSPACE_NAME + "/" + TEST_CONSTANTS.COLLECTION_NAME)
            .then()
            .statusCode(200)
            .extract()
            .asString());
  }

  private JsonObject callMcp(String command, Map<String, Object> arguments) {
    var toolArguments = new HashMap<>(arguments);
    toolArguments.put("keyspace", TEST_CONSTANTS.KEYSPACE_NAME);
    toolArguments.put("collection", TEST_CONSTANTS.COLLECTION_NAME);
    return completeResult(
        request("tools/call")
            .header("Token", TOKEN)
            .header(ApiFeature.MCP.httpHeaderName(), "true")
            .header("Mcp-Name", command)
            .body(toolPayload(1, command, toolArguments))
            .post("/v1/mcp"),
        1);
  }

  private static void assertValidationError(JsonObject result, String reason) {
    var errors = result.getJsonArray("errors");
    assertThat(errors).as(result.encode()).hasSize(1);
    var error = errors.getJsonObject(0);
    assertThat(error.getString("errorCode"))
        .as(result.encode())
        .isEqualTo(RequestException.Code.COMMAND_FIELD_VALUE_INVALID.name());
    assertThat(error.getString("message")).contains(reason);
  }

  public static class Profile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "stargate.jsonapi.operations.max-document-insert-count", "2",
          "stargate.multi-tenancy.enabled", "true",
          "stargate.multi-tenancy.tenant-resolver.type", "fixed",
          "stargate.multi-tenancy.tenant-resolver.fixed.tenant-id", "command-validation-test");
    }
  }
}
