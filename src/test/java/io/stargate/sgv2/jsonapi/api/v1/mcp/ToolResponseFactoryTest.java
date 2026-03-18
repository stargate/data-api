package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.ToolResponse;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CommandResult#toToolResponse()} verifying the CommandResult → ToolResponse
 * mapping logic.
 */
class ToolResponseFactoryTest {

  @Test
  void successWithStatusOnly() {
    // Arrange: a DDL-style result with status and no errors or data
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.OK, 1)
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse();

    // Assert: no error, no content and structuredContent, status should be mapped into _meta
    assertFalse(response.isError());
    assertThat(response.content()).isEmpty();
    assertNull(response.structuredContent());
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));
  }

  @Test
  void successWithStatusAndData() {
    // Arrange: a result with data and status
    var commandResult =
        CommandResult.singleDocumentBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.DELETED_COUNT, 0)
            .addDocument(null)
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse();

    // Assert: no error, no content, data in structuredContent, status should be mapped into _meta
    assertFalse(response.isError());
    assertThat(response.content()).isEmpty();
    assertThat(response.structuredContent()).isNotNull();
    assertInstanceOf(ResponseData.class, response.structuredContent());
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));
  }

  @Test
  void failWithErrorOnly() {
    // Arrange: a result that contains errors
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addThrowable(new RuntimeException("test error"))
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse();

    // Assert: should be an error in structuredContent, no _meta and content
    assertThat(response.content()).isEmpty();
    assertThat(response._meta()).isEmpty();
    assertTrue(response.isError());
    assertThat(response.structuredContent()).isNotNull();
    @SuppressWarnings("unchecked")
    var errorContent = (Map<String, Object>) response.structuredContent();
    assertThat(errorContent).containsKey("errors");
  }

  @Test
  void failWithErrorAndStatus() {
    // Arrange: a result that contains errors and status
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.INSERTED_IDS, 1)
            .addThrowable(new RuntimeException("test error"))
            .build();

    // Act
    ToolResponse response = commandResult.toToolResponse();

    // Assert: no content, should be an error in structuredContent, and status in _meta
    assertThat(response.content()).isEmpty();
    assertTrue(response.isError());
    assertThat(response.structuredContent()).isNotNull();
    @SuppressWarnings("unchecked")
    var errorContent = (Map<String, Object>) response.structuredContent();
    assertThat(errorContent).containsKey("errors");
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));
  }
}
