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

/**
 * MCP tool provider for database-level (general) commands. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.GeneralResource} REST endpoint.
 */
public class GeneralCommandTools {

  @Inject McpResource mcpResource;

  @Tool(description = "Command that creates a keyspace.")
  public Uni<CommandResult> createKeyspace(
      @ToolArg(description = "Required name of the new Keyspace") String name,
      @ToolArg(description = "Options for creating a new keyspace.", required = false)
          CreateKeyspaceCommand.Options options) {

    var command = new CreateKeyspaceCommand(name, options);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "Command that deletes a Keyspace.")
  public Uni<CommandResult> dropKeyspace(
      @ToolArg(description = "Required name of the Keyspace to remove") @NotEmpty String name) {

    var command = new DropKeyspaceCommand(name);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "Command that lists all available keyspaces")
  public Uni<CommandResult> findKeyspaces() {

    var command = new FindKeyspacesCommand();
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "Lists the available Embedding Providers for this database.")
  public Uni<CommandResult> findEmbeddingProviders(
      @ToolArg(
              description =
                  "Optional model status filter: SUPPORTED, DEPRECATED, or END_OF_LIFE. If omitted, only SUPPORTED models are returned.",
              required = false)
          FindEmbeddingProvidersCommand.Options options) {

    var command = new FindEmbeddingProvidersCommand(options);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }

  @Tool(description = "Lists the available Reranking Providers for this database.")
  public Uni<CommandResult> findRerankingProviders(
      @ToolArg(
              description =
                  "Optional model status filter: SUPPORTED, DEPRECATED, or END_OF_LIFE. If omitted, only SUPPORTED models are returned.",
              required = false)
          FindRerankingProvidersCommand.Options options) {

    var command = new FindRerankingProvidersCommand(options);
    var context = mcpResource.buildGeneralContext(command);
    return mcpResource.processCommand(context, command);
  }
}
