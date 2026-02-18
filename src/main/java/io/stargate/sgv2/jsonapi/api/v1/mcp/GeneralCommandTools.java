package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateKeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropKeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindKeyspacesCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotEmpty;
import javax.annotation.Nullable;

/**
 * MCP tool provider for database-level (general) commands. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.GeneralResource} REST endpoint.
 */
public class GeneralCommandTools {

  @Inject McpResource mcpResource;

  @Tool(description = "Create a new keyspace in the database")
  public Uni<CommandResult> createKeyspace(
      @ToolArg(description = "Name of the keyspace to create") String name,
      @ToolArg(
              description =
                  "Keyspace options including replication settings. "
                      + "Example: {\"replication\": {\"class\": \"SimpleStrategy\", \"replication_factor\": 1}}")
          @Nullable
          CreateKeyspaceCommand.Options options) {

    var command = new CreateKeyspaceCommand(name, options);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "Drop (delete) a keyspace from the database")
  public Uni<CommandResult> dropKeyspace(
      @ToolArg(description = "Name of the keyspace to drop") @NotEmpty String name) {

    var command = new DropKeyspaceCommand(name);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "List all keyspaces in the database")
  public Uni<CommandResult> findKeyspaces() {

    var command = new FindKeyspacesCommand();
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "List available embedding (vectorize) providers and their models")
  public Uni<CommandResult> findEmbeddingProviders(
      @ToolArg(
              description =
                  "Optional model status filter: SUPPORTED, DEPRECATED, or END_OF_LIFE. If omitted, only SUPPORTED models are returned.")
          @Nullable
          String filterModelStatus) {

    FindEmbeddingProvidersCommand.Options options = null;
    if (filterModelStatus != null && !filterModelStatus.isBlank()) {
      options = new FindEmbeddingProvidersCommand.Options(filterModelStatus);
    }

    var command = new FindEmbeddingProvidersCommand(options);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "List available reranking providers and their models")
  public Uni<CommandResult> findRerankingProviders(
      @ToolArg(
              description =
                  "Optional model status filter: SUPPORTED, DEPRECATED, or END_OF_LIFE. If omitted, only SUPPORTED models are returned.")
          @Nullable
          String filterModelStatus) {

    FindRerankingProvidersCommand.Options options = null;
    if (filterModelStatus != null && !filterModelStatus.isBlank()) {
      options = new FindRerankingProvidersCommand.Options(filterModelStatus);
    }

    var command = new FindRerankingProvidersCommand(options);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }
}
