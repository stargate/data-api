package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import jakarta.inject.Inject;
import java.util.List;

/**
 * MCP tool provider for collection-level (and table-level) commands {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand}. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.CollectionResource} REST endpoint.
 */
public class CollectionCommandTools {

  @Inject McpResource mcpResource;

  @Tool(
      description =
          "Command that finds documents based on the filter and deletes them from a collection")
  public Uni<ToolResponse> deleteMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "Filter clause based on which documents are identified")
          FilterDefinition filterDefinition) {

    var command = new DeleteManyCommand(filterDefinition);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Find a single document in a collection matching a filter")
  public Uni<ToolResponse> find(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "options", required = false) FindCommand.Options options) {

    var command = new FindCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that inserts multiple JSON document to a collection.")
  public Uni<ToolResponse> insertMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON documents to insert") List<JsonNode> documents,
      @ToolArg(description = "Options for inserting many documents")
          InsertManyCommand.Options options) {

    var command = new InsertManyCommand(documents, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that inserts a single JSON document to a collection.")
  public Uni<ToolResponse> insertOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON document to insert") JsonNode document) {

    var command = new InsertOneCommand(document);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }
}
