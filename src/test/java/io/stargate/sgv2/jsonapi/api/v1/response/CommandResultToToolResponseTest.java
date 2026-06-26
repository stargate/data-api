package io.stargate.sgv2.jsonapi.api.v1.response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.ToolResponse;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CommandResult#toToolResponse(ObjectMapper)} verifying the CommandResult →
 * ToolResponse mapping logic.
 */
public class CommandResultToToolResponseTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void successWithStatusOnly() throws Exception {
    // Arrange: a DDL-style result with status and no errors or data
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.OK, 1)
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse(OBJECT_MAPPER);

    // Assert: no error, no structuredContent, status should be mapped into _meta
    assertFalse(response.isError());
    assertNull(response.structuredContent());
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));

    // Assert: the full envelope is visible in content (what MCP clients read)
    JsonNode envelope = contentEnvelope(response);
    assertEquals(1, envelope.path("status").path("ok").asInt());
  }

  @Test
  void successWithStatusAndData() throws Exception {
    // Arrange: a result with data and status
    var commandResult =
        CommandResult.singleDocumentBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.DELETED_COUNT, 0)
            .addDocument(null)
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse(OBJECT_MAPPER);

    // Assert: no error, data in structuredContent, status should be mapped into _meta
    assertFalse(response.isError());
    assertThat(response.structuredContent()).isNotNull();
    assertInstanceOf(ResponseData.class, response.structuredContent());
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));

    // Assert: the full envelope is visible in content, with both data and status
    JsonNode envelope = contentEnvelope(response);
    assertTrue(envelope.has("data"), "Envelope should contain data");
    assertEquals(0, envelope.path("status").path("deletedCount").asInt());
  }

  @Test
  void failWithErrorOnly() throws Exception {
    // Arrange: a result that contains errors
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addThrowable(new RuntimeException("test error"))
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse(OBJECT_MAPPER);

    // Assert: should be an error in structuredContent, no _meta
    assertThat(response._meta()).isEmpty();
    assertTrue(response.isError());
    assertThat(response.structuredContent()).isNotNull();
    @SuppressWarnings("unchecked")
    var errorContent = (Map<String, Object>) response.structuredContent();
    assertThat(errorContent).containsKey("errors");

    // Assert: errors (including the message) are visible in content so agents can self-correct
    JsonNode envelope = contentEnvelope(response);
    assertThat(envelope.path("errors").isArray()).isTrue();
    assertThat(envelope.path("errors").get(0).path("message").asText()).contains("test error");
  }

  @Test
  void failWithErrorAndStatus() throws Exception {
    // Arrange: a result that contains errors and status
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.INSERTED_IDS, 1)
            .addThrowable(new RuntimeException("test error"))
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse(OBJECT_MAPPER);

    // Assert: should be an error in structuredContent, and status in _meta
    assertTrue(response.isError());
    assertThat(response.structuredContent()).isNotNull();
    @SuppressWarnings("unchecked")
    var errorContent = (Map<String, Object>) response.structuredContent();
    assertThat(errorContent).containsKey("errors");
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));

    // Assert: both errors and status are visible in content
    JsonNode envelope = contentEnvelope(response);
    assertThat(envelope.path("errors").isArray()).isTrue();
    assertTrue(envelope.has("status"), "Envelope should contain status");
  }

  /**
   * Asserts the response carries a non-empty content list whose first item is a {@link TextContent}
   * holding the JSON-serialized command result envelope, and returns the parsed envelope.
   */
  private static JsonNode contentEnvelope(ToolResponse response) throws Exception {
    assertThat(response.content())
        .as("content must not be empty: MCP clients read tool results from content")
        .isNotEmpty();
    var text = assertInstanceOf(TextContent.class, response.content().get(0));
    return OBJECT_MAPPER.readTree(text.text());
  }
}
