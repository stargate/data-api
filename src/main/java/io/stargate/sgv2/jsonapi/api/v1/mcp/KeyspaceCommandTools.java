package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import jakarta.inject.Inject;

/**
 * MCP tool provider for keyspace-level commands {@link
 * io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand}. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource} REST endpoint.
 */
public class KeyspaceCommandTools {
  @Inject McpResource mcpResource;

  @Tool(description = "Command that creates a collection.")
  public Uni<ToolResponse> createCollection(
      @ToolArg(description = "Required name of the existing keyspace") String keyspace,
      @ToolArg(description = "Required name of the new collection") String collection,
      @ToolArg(description = "Configuration options for the collection", required = false)
          CreateCollectionCommand.Options options) {

    var command = new CreateCollectionCommand(collection, options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that deletes a collection if one exists.")
  public Uni<ToolResponse> deleteCollection(
      @ToolArg(description = "Required name of the existing Keyspace") String keyspace,
      @ToolArg(description = "Required name of the existing Collection") String collection) {

    var command = new DeleteCollectionCommand(collection);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that lists all available collections in a namespace.")
  public Uni<ToolResponse> findCollections(
      @ToolArg(description = "Required name of the existing Keyspace") String keyspace,
      @ToolArg(description = "include collection properties.", required = false)
          FindCollectionsCommand.Options options) {

    var command = new FindCollectionsCommand(options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }
}
