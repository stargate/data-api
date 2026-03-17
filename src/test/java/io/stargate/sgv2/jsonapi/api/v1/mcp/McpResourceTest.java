package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link McpResource#processCommand(CommandContext, Command)} verifying the
 * CommandResult → ToolResponse mapping logic.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpResourceTest {

  @Inject McpResource mcpResource;

  @InjectMock MeteredCommandProcessor meteredCommandProcessor;

  @Test
  void successWithStatusOnly() {
    // Arrange: a DDL-style result with status and no errors and data
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.OK, 1)
            .build();

    when(meteredCommandProcessor.processCommand(any(), any()))
        .thenReturn(Uni.createFrom().item(commandResult));

    // Act
    ToolResponse response =
        mcpResource
            .processCommand(mockContextWithMcpEnabled(), mock(Command.class))
            .await()
            .indefinitely();

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

    when(meteredCommandProcessor.processCommand(any(), any()))
        .thenReturn(Uni.createFrom().item(commandResult));

    // Act
    ToolResponse response =
        mcpResource
            .processCommand(mockContextWithMcpEnabled(), mock(Command.class))
            .await()
            .indefinitely();

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

    when(meteredCommandProcessor.processCommand(any(), any()))
        .thenReturn(Uni.createFrom().item(commandResult));

    // Act
    ToolResponse response =
        mcpResource
            .processCommand(mockContextWithMcpEnabled(), mock(Command.class))
            .await()
            .indefinitely();

    // Assert: should be an error in structuredContent, no meta_ and content
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

    when(meteredCommandProcessor.processCommand(any(), any()))
        .thenReturn(Uni.createFrom().item(commandResult));

    // Act
    ToolResponse response =
        mcpResource
            .processCommand(mockContextWithMcpEnabled(), mock(Command.class))
            .await()
            .indefinitely();

    // Assert: no content, should be an error in structuredContent, and status in meta_
    assertThat(response.content()).isEmpty();
    assertTrue(response.isError());
    assertThat(response.structuredContent()).isNotNull();
    @SuppressWarnings("unchecked")
    var errorContent = (Map<String, Object>) response.structuredContent();
    assertThat(errorContent).containsKey("errors");
    assertNotNull(response._meta());
    assertNotNull(response._meta().get(MetaKey.of("status")));
  }

  @Test
  void failWithMCPFeatureDisabled() {
    // Arrange & Act: context with MCP feature disabled
    ToolResponse response =
        mcpResource
            .processCommand(mockContextWithMcpDisabled(), mock(Command.class))
            .await()
            .indefinitely();

    // Assert: should return error ToolResponse without calling processCommand
    assertThat(response.content()).isEmpty();
    assertThat(response._meta()).isEmpty();
    assertThat(response.isError()).isTrue();
    assertThat(response.structuredContent()).isNotNull();
    @SuppressWarnings("unchecked")
    var errorContent = (Map<String, Object>) response.structuredContent();
    assertThat(errorContent).containsKey("errors");

    // processCommand should never have been called
    verify(meteredCommandProcessor, never()).processCommand(any(), any());
  }

  /** Create a mock CommandContext with MCP feature enabled. */
  @SuppressWarnings("unchecked")
  private Uni<CommandContext<?>> mockContextWithMcpEnabled() {
    CommandContext<DatabaseSchemaObject> ctx = mock(CommandContext.class);
    ApiFeatures features = mock(ApiFeatures.class);
    when(features.isFeatureEnabled(ApiFeature.MCP)).thenReturn(true);
    when(ctx.apiFeatures()).thenReturn(features);
    when(ctx.requestTracing()).thenReturn(RequestTracing.NO_OP);
    return Uni.createFrom().item(ctx);
  }

  /** Create a mock CommandContext with MCP feature disabled. */
  @SuppressWarnings("unchecked")
  private Uni<CommandContext<?>> mockContextWithMcpDisabled() {
    CommandContext<DatabaseSchemaObject> ctx = mock(CommandContext.class);
    ApiFeatures features = mock(ApiFeatures.class);
    when(features.isFeatureEnabled(ApiFeature.MCP)).thenReturn(false);
    when(ctx.apiFeatures()).thenReturn(features);
    when(ctx.requestTracing()).thenReturn(RequestTracing.NO_OP);
    return Uni.createFrom().item(ctx);
  }
}
