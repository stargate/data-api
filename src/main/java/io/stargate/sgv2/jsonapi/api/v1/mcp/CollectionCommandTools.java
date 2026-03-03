package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolResponse;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexType;
import jakarta.inject.Inject;
import java.util.List;

/**
 * MCP tool provider for collection-level (and table-level) commands {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand}. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.CollectionResource} REST endpoint.
 */
public class CollectionCommandTools {

  @Inject McpResource mcpResource;

  @Tool(description = "Creates a regular index for a column in a table.")
  public Uni<ToolResponse> createIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "Required name of the new Index") String indexName,
      @ToolArg(description = "Definition of the index to create")
          RegularIndexDefinitionDesc definition,
      @ToolArg(
              description =
                  "Optional type of the index to create. The only supported value is '"
                      + ApiIndexType.Constants.REGULAR
                      + "'.",
              required = false)
          String indexType,
      @ToolArg(description = "Options for the command.", required = false)
          CreateIndexCommand.CreateIndexCommandOptions options) {

    var command = new CreateIndexCommand(indexName, definition, indexType, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds documents based on the filter and deletes them from a collection")
  public Uni<ToolResponse> deleteMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "Filter clause based on which documents are identified")
          FilterDefinition filterDefinition) {

    var command = new DeleteManyCommand(filterDefinition);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Finds documents using using vector and lexical sorting, then reranks the results.")
  public Uni<ToolResponse> findAndRerank(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "sort", required = false) FindAndRerankSort sort,
      @ToolArg(description = "options", required = false) FindAndRerankCommand.Options options) {

    var command = new FindAndRerankCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that finds JSON documents from a collection.")
  public Uni<ToolResponse> find(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "options", required = false) FindCommand.Options options) {

    var command = new FindCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that finds a single JSON document from a collection.")
  public Uni<ToolResponse> findOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "options", required = false) FindOneCommand.Options options) {

    var command = new FindOneCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a table or collection and updates the value provided in the update clause.")
  public Uni<ToolResponse> updateOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter") FilterDefinition filter,
      @ToolArg(description = "update") UpdateClause update,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "options", required = false) UpdateOneCommand.Options options) {

    var command = new UpdateOneCommand(filter, update, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that inserts multiple JSON document to a collection.")
  public Uni<ToolResponse> insertMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "JSON documents to insert") List<JsonNode> documents,
      @ToolArg(description = "Options for inserting many documents", required = false)
          InsertManyCommand.Options options) {

    var command = new InsertManyCommand(documents, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that inserts a single JSON document to a collection.")
  public Uni<ToolResponse> insertOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "JSON document to insert") JsonNode document) {

    var command = new InsertOneCommand(document);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }
}
