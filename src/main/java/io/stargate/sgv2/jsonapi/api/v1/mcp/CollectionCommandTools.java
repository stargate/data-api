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
 *
 * <p>The filter and sort arguments have no description here. A {@code @ToolArg} description would
 * replace the one on their classes, which Swagger also uses.
 */
public class CollectionCommandTools {

  @Inject McpResource mcpResource;

  @Tool(description = "Command that alters the column definition in a table.")
  public Uni<ToolResponse> alterTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(
              description =
                  "One change to the table, keyed by add, drop, addVectorize, or dropVectorize, for example {\"add\": {\"columns\": {\"rating\": \"int\"}}}")
          AlterTableOperation operation) {

    var command = new AlterTableCommand(operation);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(description = "Command that returns count of documents in a collection")
  public Uni<ToolResponse> countDocuments(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(required = false) FilterDefinition filter) {

    var command = new CountDocumentsCommand(filter);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Creates a regular index for a column in a table.")
  public Uni<ToolResponse> createIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "Name of the new index") String indexName,
      @ToolArg(
              description =
                  "Column to index and its index options, for example {\"column\": \"status\"}")
          RegularIndexDefinitionDesc definition,
      @ToolArg(
              description =
                  "Type of the index to create. The only supported value is '"
                      + ApiIndexType.Constants.REGULAR
                      + "'.",
              required = false)
          String indexType,
      @ToolArg(
              description =
                  "Set ifNotExists to true to succeed when an index with this name already exists",
              required = false)
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
      @ToolArg(description = "Name of the new index") String indexName,
      @ToolArg(
              description =
                  "Column to index and its index options, for example {\"column\": \"title\"}")
          TextIndexDefinitionDesc definition,
      @ToolArg(
              description =
                  "Type of the index to create. The only supported value is '"
                      + ApiIndexType.Constants.TEXT
                      + "'.",
              required = false)
          String indexType,
      @ToolArg(
              description =
                  "Set ifNotExists to true to succeed when an index with this name already exists",
              required = false)
          CreateTextIndexCommand.CommandOptions options) {

    var command = new CreateTextIndexCommand(indexName, definition, indexType, options);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(description = "Creates an index on a vector column that can be used for vector sorting.")
  public Uni<ToolResponse> createVectorIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(description = "Name of the new index") String indexName,
      @ToolArg(
              description =
                  "Column to index and its index options, for example {\"column\": \"embedding\", \"options\": {\"metric\": \"cosine\"}}")
          VectorIndexDefinitionDesc definition,
      @ToolArg(
              description =
                  "Type of the index to create. The only supported value is '"
                      + ApiIndexType.Constants.VECTOR
                      + "'.",
              required = false)
          String indexType,
      @ToolArg(
              description =
                  "Set ifNotExists to true to succeed when an index with this name already exists",
              required = false)
          CreateVectorIndexCommand.CreateVectorIndexCommandOptions options) {

    var command = new CreateVectorIndexCommand(indexName, definition, indexType, options);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(
      description =
          "Command that finds documents based on the filter and deletes them from a collection")
  public Uni<ToolResponse> deleteMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg(required = false) FilterDefinition filter) {

    var command = new DeleteManyCommand(filter);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that finds a single document and deletes it from a collection")
  public Uni<ToolResponse> deleteOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg FilterDefinition filter,
      @ToolArg(required = false) SortDefinition sort) {

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

  @Tool(description = "Finds documents using vector and lexical sorting, then reranks the results.")
  public Uni<ToolResponse> findAndRerank(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(
              description =
                  "Fields to include or exclude in the returned documents, for example {\"title\": 1}",
              required = false)
          JsonNode projection,
      @ToolArg(required = false) FindAndRerankSort sort,
      @ToolArg(
              description = "Options for result limits, reranking, and what the response includes",
              required = false)
          FindAndRerankCommand.Options options) {

    var command = new FindAndRerankCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that finds JSON documents from a collection.")
  public Uni<ToolResponse> find(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(
              description =
                  "Fields to include or exclude in the returned documents, for example {\"title\": 1}",
              required = false)
          JsonNode projection,
      @ToolArg(required = false) SortDefinition sort,
      @ToolArg(
              description = "Options for result limits, paging, and what the response includes",
              required = false)
          FindCommand.Options options) {

    var command = new FindCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a collection and deletes it. The deleted document is returned")
  public Uni<ToolResponse> findOneAndDelete(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(required = false) SortDefinition sort,
      @ToolArg(
              description =
                  "Fields to include or exclude in the returned document, for example {\"title\": 1}",
              required = false)
          JsonNode projection) {

    var command = new FindOneAndDeleteCommand(filter, sort, projection);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a collection and replaces it with the replacement document.")
  public Uni<ToolResponse> findOneAndReplace(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(required = false) SortDefinition sort,
      @ToolArg(
              description =
                  "Fields to include or exclude in the returned document, for example {\"title\": 1}",
              required = false)
          JsonNode projection,
      @ToolArg(
              description =
                  "Document that replaces the matching document, for example {\"title\": \"Dune\", \"status\": \"active\"}")
          ObjectNode replacement,
      @ToolArg(
              description =
                  "Options for upsert and whether to return the document before or after the replacement",
              required = false)
          FindOneAndReplaceCommand.Options options) {

    var command = new FindOneAndReplaceCommand(filter, sort, projection, replacement, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a collection and updates the values provided in the update clause.")
  public Uni<ToolResponse> findOneAndUpdate(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(
              description =
                  "Fields to include or exclude in the returned document, for example {\"title\": 1}",
              required = false)
          JsonNode projection,
      @ToolArg(required = false) SortDefinition sort,
      @ToolArg(
              description =
                  "Update operators to apply, for example {\"$set\": {\"status\": \"active\"}}")
          UpdateClause update,
      @ToolArg(
              description =
                  "Options for upsert and whether to return the document before or after the update",
              required = false)
          FindOneAndUpdateCommand.Options options) {

    var command = new FindOneAndUpdateCommand(filter, projection, sort, update, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that finds a single JSON document from a collection.")
  public Uni<ToolResponse> findOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(
              description =
                  "Fields to include or exclude in the returned document, for example {\"title\": 1}",
              required = false)
          JsonNode projection,
      @ToolArg(required = false) SortDefinition sort,
      @ToolArg(description = "Options for what the response includes", required = false)
          FindOneCommand.Options options) {

    var command = new FindOneCommand(filter, projection, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that lists all available indexes in a table.")
  public Uni<ToolResponse> listIndexes(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String table,
      @ToolArg(
              description = "Set explain to true to include the properties of each index",
              required = false)
          ListIndexesCommand.Options options) {

    var command = new ListIndexesCommand(options);
    return mcpResource.processCollectionCommand(keyspace, table, command);
  }

  @Tool(description = "Command that inserts multiple JSON documents to a collection.")
  public Uni<ToolResponse> insertMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg(
              description =
                  "Documents or rows to insert, for example [{\"title\": \"Dune\"}, {\"title\": \"Emma\"}]")
          List<JsonNode> documents,
      @ToolArg(description = "Options for insert order and per-document results", required = false)
          InsertManyCommand.Options options) {

    var command = new InsertManyCommand(documents, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Command that inserts a single JSON document to a collection.")
  public Uni<ToolResponse> insertOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg(
              description =
                  "Document or row to insert, for example {\"title\": \"Dune\", \"status\": \"active\"}")
          JsonNode document) {

    var command = new InsertOneCommand(document);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds documents from a collection and updates it with the values provided in the update clause.")
  public Uni<ToolResponse> updateMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(
              description =
                  "Update operators to apply, for example {\"$set\": {\"status\": \"active\"}}")
          UpdateClause update,
      @ToolArg(description = "Options for upsert and paging", required = false)
          UpdateManyCommand.Options options) {

    var command = new UpdateManyCommand(filter, update, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(
      description =
          "Command that finds a single JSON document from a table or collection and updates the values provided in the update clause.")
  public Uni<ToolResponse> updateOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection or table") String collection,
      @ToolArg(required = false) FilterDefinition filter,
      @ToolArg(
              description =
                  "Update operators to apply, for example {\"$set\": {\"status\": \"active\"}}")
          UpdateClause update,
      @ToolArg(required = false) SortDefinition sort,
      @ToolArg(
              description = "Set upsert to true to insert a document when none matches the filter",
              required = false)
          UpdateOneCommand.Options options) {

    var command = new UpdateOneCommand(filter, update, sort, options);
    return mcpResource.processCollectionCommand(keyspace, collection, command);
  }
}
