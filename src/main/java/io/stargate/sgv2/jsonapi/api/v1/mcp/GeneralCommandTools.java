package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
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
  public Uni<ToolResponse> createKeyspace(
      @ToolArg(description = "Required name of the new Keyspace") String name,
      @ToolArg(description = "Options for creating a new keyspace.", required = false)
          CreateKeyspaceCommand.Options options) {

    var command = new CreateKeyspaceCommand(name, options);
    return mcpResource.processCommand(mcpResource.buildGeneralContext(command), command);
  }

  @Tool(description = "Command that deletes a Keyspace.")
  public Uni<ToolResponse> dropKeyspace(
      @ToolArg(description = "Required name of the Keyspace to remove") @NotEmpty String name) {

    var command = new DropKeyspaceCommand(name);
    return mcpResource.processCommand(mcpResource.buildGeneralContext(command), command);
  }

  @Tool(description = "Command that lists all available keyspaces")
  public Uni<ToolResponse> findKeyspaces() {

    var command = new FindKeyspacesCommand();
    return mcpResource.processCommand(mcpResource.buildGeneralContext(command), command);
  }

  @Tool(description = "Lists the available Embedding Providers for this database.")
  public Uni<ToolResponse> findEmbeddingProviders(
      @ToolArg(
              description =
                  "Optional model status filter: SUPPORTED, DEPRECATED, or END_OF_LIFE. If omitted, only SUPPORTED models are returned.",
              required = false)
          FindEmbeddingProvidersCommand.Options options) {

    var command = new FindEmbeddingProvidersCommand(options);
    return mcpResource.processCommand(mcpResource.buildGeneralContext(command), command);
  }

  @Tool(description = "Lists the available Reranking Providers for this database.")
  public Uni<ToolResponse> findRerankingProviders(
      @ToolArg(
              description =
                  "Optional model status filter: SUPPORTED, DEPRECATED, or END_OF_LIFE. If omitted, only SUPPORTED models are returned.",
              required = false)
          FindRerankingProvidersCommand.Options options) {

    var command = new FindRerankingProvidersCommand(options);
    return mcpResource.processCommand(mcpResource.buildGeneralContext(command), command);
  }
}
