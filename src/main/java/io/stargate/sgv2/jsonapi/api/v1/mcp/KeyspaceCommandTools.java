package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.RenameTypeFieldsDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDefinitionDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
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

  @Tool(description = "Command that lists all available collections in a keyspace.")
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
  @Tool(description = "Command that alters a user defined type.")
  public Uni<ToolResponse> alterType(
      @ToolArg(description = "Keyspace of the type to alter") String keyspace,
      @ToolArg(description = "Name of the type to alter") String name,
      @ToolArg(description = "Operation to rename fields", required = false)
          RenameTypeFieldsDesc rename,
      @ToolArg(description = "Operation to add fields", required = false) TypeDefinitionDesc add) {

    var command = new AlterTypeCommand(name, rename, add);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

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

  @Tool(description = "Command that creates a user defined type.")
  public Uni<ToolResponse> createType(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Name of the type to create") String name,
      @ToolArg(description = "Type definition") TypeDefinitionDesc definition,
      @ToolArg(description = "Configuration options for the command", required = false)
          CreateTypeCommand.Options options) {

    var command = new CreateTypeCommand(name, definition, options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(description = "Command that drops an index for a column.")
  public Uni<ToolResponse> dropIndex(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Name of the Index to remove") String name,
      @ToolArg(description = "Dropping index command option.", required = false)
          DropIndexCommand.Options options) {

    var command = new DropIndexCommand(name, options);
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

  @Tool(description = "Command that drops a user defined type if one exists.")
  public Uni<ToolResponse> dropType(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Name of the Type to remove") String name,
      @ToolArg(description = "Options for drop command", required = false)
          DropTypeCommand.Options options) {

    var command = new DropTypeCommand(name, options);
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

  @Tool(description = "Command that lists all available types in a keyspace")
  public Uni<ToolResponse> listTypes(
      @ToolArg(description = "Name of the existing keyspace") String keyspace,
      @ToolArg(description = "Options for the `listTypes` command.", required = false)
          ListTypesCommand.Options options) {

    var command = new ListTypesCommand(options);
    return mcpResource.processCommand(mcpResource.buildKeyspaceContext(keyspace, command), command);
  }
}
