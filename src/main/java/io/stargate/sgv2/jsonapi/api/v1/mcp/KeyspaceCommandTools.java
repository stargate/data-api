package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDefinitionDesc;
import jakarta.inject.Inject;

/**
 * MCP tool provider for keyspace-level commands {@link
 * io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand}. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource} REST endpoint.
 */
public class KeyspaceCommandTools {
  @Inject McpResource mcpResource;

  /**
   * Below are converted from {@link
   * io.stargate.sgv2.jsonapi.api.model.command.CollectionOnlyCommand}
   */
  @Tool(description = "Command that creates a collection.")
  public Uni<ToolResponse> createCollection(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Name of the new collection") String collection,
      @ToolArg(description = "Configuration options for the collection", required = false)
          CreateCollectionCommand.Options options) {

    var command = new CreateCollectionCommand(collection, options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that deletes a collection if one exists.")
  public Uni<ToolResponse> deleteCollection(
      @ToolArg(description = "Name of the existing Keyspace") String keyspace,
      @ToolArg(description = "Name of the existing Collection") String collection) {

    var command = new DeleteCollectionCommand(collection);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that lists all available collections in a namespace.")
  public Uni<ToolResponse> findCollections(
      @ToolArg(description = "Name of the existing Keyspace") String keyspace,
      @ToolArg(description = "include collection properties.", required = false)
          FindCollectionsCommand.Options options) {

    var command = new FindCollectionsCommand(options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  /**
   * Below are converted from {@link io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand}
   */
  @Tool(description = "Command that creates an API Table.")
  public Uni<ToolResponse> createTable(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Name of the new Table") String table,
      @ToolArg(description = "Table definition") TableDefinitionDesc definition,
      @ToolArg(description = "Configuration options for the table", required = false)
          CreateTableCommand.Options options) {

    var command = new CreateTableCommand(table, definition, options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that drops a table if one exists.")
  public Uni<ToolResponse> dropTable(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Name of the Table to remove") String table,
      @ToolArg(description = "Optional options for drop command", required = false)
          DropTableCommand.Options options) {

    var command = new DropTableCommand(table, options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that lists all available tables in a keyspace")
  public Uni<ToolResponse> listTables(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Options for the `listTables` command.", required = false)
          ListTablesCommand.Options options) {

    var command = new ListTablesCommand(options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }
}
