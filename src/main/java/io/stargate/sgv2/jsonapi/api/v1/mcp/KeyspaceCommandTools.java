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
  @Tool(
      description = "Command that creates a collection.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = false,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> createCollection(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the new collection") String collection,
      @ToolArg(
              description =
                  "Collection settings for document IDs, vector search, indexing, lexical search, and reranking",
              required = false)
          CreateCollectionCommand.Options options) {

    var command = new CreateCollectionCommand(collection, options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that deletes a collection if one exists.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = true,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> deleteCollection(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection to delete") String collection) {

    var command = new DeleteCollectionCommand(collection);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that lists all available collections in a keyspace.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = true,
              destructiveHint = false,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> findCollections(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(
              description = "Set explain to true to include the properties of each collection",
              required = false)
          FindCollectionsCommand.Options options) {

    var command = new FindCollectionsCommand(options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  /**
   * Below are converted from {@link io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand}
   */
  @Tool(
      description = "Command that alters a user defined type.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = true,
              idempotentHint = false,
              openWorldHint = false))
  public Uni<ToolResponse> alterType(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the type to alter") String name,
      @ToolArg(description = "Fields to rename, mapping old names to new names", required = false)
          RenameTypeFieldsDesc rename,
      @ToolArg(description = "Fields to add to the type", required = false)
          TypeDefinitionDesc add) {

    var command = new AlterTypeCommand(name, rename, add);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that creates an API Table.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = false,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> createTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the new table") String table,
      @ToolArg(description = "Columns and primary key of the new table")
          TableDefinitionDesc definition,
      @ToolArg(
              description = "Set ifNotExists to true to succeed when the table already exists",
              required = false)
          CreateTableCommand.Options options) {

    var command = new CreateTableCommand(table, definition, options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that creates a user defined type.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = false,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> createType(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the new type") String name,
      @ToolArg(description = "Fields of the new type") TypeDefinitionDesc definition,
      @ToolArg(
              description = "Set ifNotExists to true to succeed when the type already exists",
              required = false)
          CreateTypeCommand.Options options) {

    var command = new CreateTypeCommand(name, definition, options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that drops an index for a column.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = true,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> dropIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the index to drop") String name,
      @ToolArg(
              description = "Set ifExists to true to succeed when the index does not exist",
              required = false)
          DropIndexCommand.Options options) {

    var command = new DropIndexCommand(name, options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that drops a table if one exists.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = true,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> dropTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table to drop") String table,
      @ToolArg(
              description = "Set ifExists to true to succeed when the table does not exist",
              required = false)
          DropTableCommand.Options options) {

    var command = new DropTableCommand(table, options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that drops a user defined type if one exists.",
      annotations =
          @Tool.Annotations(
              readOnlyHint = false,
              destructiveHint = true,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> dropType(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the type to drop") String name,
      @ToolArg(
              description = "Set ifExists to true to succeed when the type does not exist",
              required = false)
          DropTypeCommand.Options options) {

    var command = new DropTypeCommand(name, options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that lists all available tables in a keyspace",
      annotations =
          @Tool.Annotations(
              readOnlyHint = true,
              destructiveHint = false,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> listTables(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(
              description = "Set explain to true to include the properties of each table",
              required = false)
          ListTablesCommand.Options options) {

    var command = new ListTablesCommand(options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }

  @Tool(
      description = "Command that lists all available types in a keyspace",
      annotations =
          @Tool.Annotations(
              readOnlyHint = true,
              destructiveHint = false,
              idempotentHint = true,
              openWorldHint = false))
  public Uni<ToolResponse> listTypes(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(
              description = "Set explain to true to include the fields of each type",
              required = false)
          ListTypesCommand.Options options) {

    var command = new ListTypesCommand(options);
    return mcpResource.processCommand(
        () -> mcpResource.buildKeyspaceContext(keyspace, command), command);
  }
}
