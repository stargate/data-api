package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import jakarta.inject.Inject;
import javax.annotation.Nullable;

/**
 * MCP tool provider for keyspace-level commands. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource} REST endpoint.
 */
public class KeyspaceCommandTools {

  @Inject McpToolsHelper helper;

  @Tool(
      description =
          "Create a new collection in a keyspace. Options is a JSON object with optional fields: vector, indexing, defaultId, lexical, rerank.")
  public Uni<String> createCollection(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection to create") String name,
      @ToolArg(
              description =
                  "Optional JSON object with collection options, e.g. {\"vector\": {\"dimension\": 1024, \"metric\": \"cosine\"}, \"indexing\": {\"deny\": [\"field1\"]}}")
          @Nullable
          String options) {

    CreateCollectionCommand command;
    if (options != null && !options.isBlank()) {
      // Deserialize the full options via Jackson
      var opts = helper.deserializeCommand(options, CreateCollectionCommand.Options.class);
      command = new CreateCollectionCommand(name, opts);
    } else {
      command = new CreateCollectionCommand(name, null);
    }

    var context = helper.buildKeyspaceContext(keyspace, "CreateCollectionCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "Delete (drop) a collection from a keyspace")
  public Uni<String> deleteCollection(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection to delete") String name) {

    var command = new DeleteCollectionCommand(name);
    var context = helper.buildKeyspaceContext(keyspace, "DeleteCollectionCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "List all collections in a keyspace")
  public Uni<String> findCollections(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "If true, include collection configuration details") @Nullable
          Boolean explain) {

    FindCollectionsCommand.Options options = null;
    if (explain != null) {
      options = new FindCollectionsCommand.Options(explain);
    }

    var command = new FindCollectionsCommand(options);
    var context = helper.buildKeyspaceContext(keyspace, "FindCollectionsCommand");
    return helper.processCommand(context, command);
  }

  @Tool(
      description =
          "Create a new table in a keyspace. Definition is a JSON object describing columns and primary key.")
  public Uni<String> createTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table to create") String name,
      @ToolArg(
              description =
                  "JSON object of the full createTable command body, e.g. {\"name\": \"myTable\", \"definition\": {...}, \"options\": {\"ifNotExists\": true}}")
          String commandJson) {

    // CreateTableCommand has complex nested types, use Jackson deserialization
    var command = helper.deserializeCommand(commandJson, CreateTableCommand.class);
    var context = helper.buildKeyspaceContext(keyspace, "CreateTableCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "Drop (delete) a table from a keyspace")
  public Uni<String> dropTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table to drop") String name,
      @ToolArg(description = "If true, do not error if the table does not exist") @Nullable
          Boolean ifExists) {

    DropTableCommand.Options options = null;
    if (ifExists != null) {
      options = new DropTableCommand.Options(ifExists);
    }
    var command = new DropTableCommand(name, options);
    var context = helper.buildKeyspaceContext(keyspace, "DropTableCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "Drop (delete) an index from a keyspace")
  public Uni<String> dropIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the index to drop") String name,
      @ToolArg(description = "If true, do not error if the index does not exist") @Nullable
          Boolean ifExists) {

    DropIndexCommand.Options options = null;
    if (ifExists != null) {
      options = new DropIndexCommand.Options(ifExists);
    }
    var command = new DropIndexCommand(name, options);
    var context = helper.buildKeyspaceContext(keyspace, "DropIndexCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "List all tables in a keyspace")
  public Uni<String> listTables(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "If true, include table configuration details") @Nullable
          Boolean explain) {

    ListTablesCommand.Options options = null;
    if (explain != null) {
      options = new ListTablesCommand.Options(explain);
    }
    var command = new ListTablesCommand(options);
    var context = helper.buildKeyspaceContext(keyspace, "ListTablesCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "List all user-defined types (UDTs) in a keyspace")
  public Uni<String> listTypes(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "If true, include type definition details") @Nullable
          Boolean explain) {

    ListTypesCommand.Options options = null;
    if (explain != null) {
      options = new ListTypesCommand.Options(explain);
    }
    var command = new ListTypesCommand(options);
    var context = helper.buildKeyspaceContext(keyspace, "ListTypesCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "Create a new user-defined type (UDT) in a keyspace")
  public Uni<String> createType(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(
              description =
                  "JSON object of the full createType command body, e.g. {\"name\": \"address\", \"definition\": {\"fields\": {\"street\": \"text\", \"city\": \"text\"}}, \"options\": {\"ifNotExists\": true}}")
          String commandJson) {

    var command = helper.deserializeCommand(commandJson, CreateTypeCommand.class);
    var context = helper.buildKeyspaceContext(keyspace, "CreateTypeCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "Alter an existing user-defined type (UDT) in a keyspace")
  public Uni<String> alterType(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(
              description =
                  "JSON object of the full alterType command body, e.g. {\"name\": \"address\", \"operation\": {\"add\": {\"zipcode\": \"text\"}}}")
          String commandJson) {

    var command = helper.deserializeCommand(commandJson, AlterTypeCommand.class);
    var context = helper.buildKeyspaceContext(keyspace, "AlterTypeCommand");
    return helper.processCommand(context, command);
  }

  @Tool(description = "Drop (delete) a user-defined type (UDT) from a keyspace")
  public Uni<String> dropType(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the type to drop") String name,
      @ToolArg(description = "If true, do not error if the type does not exist") @Nullable
          Boolean ifExists) {

    DropTypeCommand.Options options = null;
    if (ifExists != null) {
      options = new DropTypeCommand.Options(ifExists);
    }
    var command = new DropTypeCommand(name, options);
    var context = helper.buildKeyspaceContext(keyspace, "DropTypeCommand");
    return helper.processCommand(context, command);
  }
}
