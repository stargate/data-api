package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.TextIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.VectorIndexDefinitionDesc;
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

  @Tool(description = "Command that alters the column definition in a table.")
  public Uni<ToolResponse> alterTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "The alter table operation") AlterTableOperation operation) {

    var command = new AlterTableCommand(operation);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(description = "Command that returns count of documents in a collection")
  public Uni<ToolResponse> countDocuments(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter) {

    var command = new CountDocumentsCommand(filter);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Creates a regular index for a column in a table.")
  public Uni<ToolResponse> createIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "Name of the new Index") String indexName,
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
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(
      description =
          "Creates an index on a text column that can be used for lexical filtering and sorting.")
  public Uni<ToolResponse> createTextIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "Name of the new Index") String indexName,
      @ToolArg(description = "Definition of the index to create")
          TextIndexDefinitionDesc definition,
      @ToolArg(
              description =
                  "Optional type of the index to create. The only supported value is '"
                      + ApiIndexType.Constants.TEXT
                      + "'.",
              required = false)
          String indexType,
      @ToolArg(description = "Options for the command.", required = false)
          CreateTextIndexCommand.CommandOptions options) {

    var command = new CreateTextIndexCommand(indexName, definition, indexType, options);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(description = "Creates an index on a vector column that can be used for vector sorting.")
  public Uni<ToolResponse> createVectorIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "Name of the new Index") String indexName,
      @ToolArg(description = "Definition of the index to create")
          VectorIndexDefinitionDesc definition,
      @ToolArg(
              description =
                  "Optional type of the index to create. The only supported value is '"
                      + ApiIndexType.Constants.VECTOR
                      + "'.",
              required = false)
          String indexType,
      @ToolArg(description = "Options for the command.", required = false)
          CreateVectorIndexCommand.CreateVectorIndexCommandOptions options) {

    var command = new CreateVectorIndexCommand(indexName, definition, indexType, options);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(
      description =
          "Command that finds documents based on the filter and deletes them from a collection")
  public Uni<ToolResponse> deleteMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(
              description = "Filter clause based on which documents are identified",
              required = false)
          FilterDefinition filter) {

    var command = new DeleteManyCommand(filter);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that finds a single document and deletes it from a collection")
  public Uni<ToolResponse> deleteOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "Filter clause based on which documents are identified")
          FilterDefinition filter,
      @ToolArg(description = "Sort clause", required = false) SortDefinition sort) {

    var command = new DeleteOneCommand(filter, sort);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that returns estimated count of documents in a collection")
  public Uni<ToolResponse> estimatedDocumentCount(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection) {

    var command = new EstimatedDocumentCountCommand();
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

  @Tool(
      description =
          "Command that finds a single JSON document from a collection and deletes it. The deleted document is returned")
  public Uni<ToolResponse> findOneAndDelete(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "projection", required = false) JsonNode projection) {

    var command = new FindOneAndDeleteCommand(filter, sort, projection);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a collection and replaces it with the replacement document.")
  public Uni<ToolResponse> findOneAndReplace(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "replacement") ObjectNode replacement,
      @ToolArg(description = "options", required = false)
          FindOneAndReplaceCommand.Options options) {

    var command = new FindOneAndReplaceCommand(filter, sort, projection, replacement, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a collection and updates the values provided in the update clause.")
  public Uni<ToolResponse> findOneAndUpdate(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "projection", required = false) JsonNode projection,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "update") UpdateClause update,
      @ToolArg(description = "options", required = false) FindOneAndUpdateCommand.Options options) {

    var command = new FindOneAndUpdateCommand(filter, projection, sort, update, options);
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

  @Tool(description = "Command that lists all available indexes in a table.")
  public Uni<ToolResponse> listIndexes(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "Options for the `listIndexes` command.", required = false)
          ListIndexesCommand.Options options) {

    var command = new ListIndexesCommand(options);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(description = "Command that inserts multiple JSON documents to a collection.")
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

  @Tool(
      description =
          "Command that finds documents from a collection and updates it with the values provided in the update clause.")
  public Uni<ToolResponse> updateMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "update") UpdateClause update,
      @ToolArg(description = "options", required = false) UpdateManyCommand.Options options) {

    var command = new UpdateManyCommand(filter, update, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a table or collection and updates the values provided in the update clause.")
  public Uni<ToolResponse> updateOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection/table") String collection,
      @ToolArg(description = "filter", required = false) FilterDefinition filter,
      @ToolArg(description = "update") UpdateClause update,
      @ToolArg(description = "sort", required = false) SortDefinition sort,
      @ToolArg(description = "options", required = false) UpdateOneCommand.Options options) {

    var command = new UpdateOneCommand(filter, update, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }
}
