package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateKeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropKeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindKeyspacesCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotEmpty;

/**
 * MCP tool provider for database-level (general) commands. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.GeneralResource} REST endpoint.
 */
public class GeneralCommandTools {

  @Inject McpToolsHelper helper;

  @Tool(description = "Create a new keyspace in the database")
  public Uni<String> createKeyspace(
      @ToolArg(description = "Name of the keyspace to create") String name,
      @ToolArg(
              description = "Replication strategy class: SimpleStrategy or NetworkTopologyStrategy")
          String replicationClass,
      @ToolArg(description = "Replication factor (for SimpleStrategy)") String replicationFactor) {

    CreateKeyspaceCommand.Options options = null;
    if (replicationClass != null && !replicationClass.isBlank()) {
      // Use Jackson deserialization for full command construction
      String optionsJson =
          "{\"replication\": {\"class\": \"%s\"%s}}"
              .formatted(
                  replicationClass,
                  (replicationFactor != null && !replicationFactor.isBlank())
                      ? ", \"replication_factor\": " + replicationFactor
                      : "");
      options = helper.deserializeCommand(optionsJson, CreateKeyspaceCommand.Options.class);
    }

    var command = new CreateKeyspaceCommand(name, options);
    var context = helper.buildGeneralContext(command);
    return helper.processCommand(context, command);
  }

  @Tool(description = "Drop (delete) a keyspace from the database")
  public Uni<String> dropKeyspace(
      @ToolArg(description = "Name of the keyspace to drop") @NotEmpty String name) {

    var command = new DropKeyspaceCommand(name);
    var context = helper.buildGeneralContext(command);
    return helper.processCommand(context, command);
  }

  @Tool(description = "List all keyspaces in the database")
  public Uni<String> findKeyspaces() {

    var command = new FindKeyspacesCommand();
    var context = helper.buildGeneralContext(command);
    return helper.processCommand(context, command);
  }

  @Tool(description = "List available embedding (vectorize) providers and their models")
  public Uni<String> findEmbeddingProviders(
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
    var context = helper.buildGeneralContext(command);
    return helper.processCommand(context, command);
  }

  @Tool(description = "List available reranking providers and their models")
  public Uni<String> findRerankingProviders(
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
    var context = helper.buildGeneralContext(command);
    return helper.processCommand(context, command);
  }
}
