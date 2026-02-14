package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * MCP tool provider for collection-level (and table-level) commands. These correspond to the {@link
 * io.stargate.sgv2.jsonapi.api.v1.CollectionResource} REST endpoint.
 */
public class CollectionCommandTools {

  @Inject McpToolsHelper helper;

  // ---- Document Read Operations ----

  @Tool(description = "Find a single document in a collection matching a filter")
  public Uni<String> findOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter, e.g. {\"name\": \"John\"}") @Nullable
          String filter,
      @ToolArg(description = "JSON object projection, e.g. {\"name\": 1, \"age\": 1}") @Nullable
          String projection,
      @ToolArg(description = "JSON object sort, e.g. {\"age\": 1}") @Nullable String sort,
      @ToolArg(description = "Include similarity score in response") @Nullable
          Boolean includeSimilarity,
      @ToolArg(description = "Return vector embedding used for ANN sorting") @Nullable
          Boolean includeSortVector) {

    var filterDef = helper.toFilterDefinition(filter);
    var projectionNode = helper.parseJsonNode(projection);
    var sortDef = helper.toSortDefinition(sort);

    FindOneCommand.Options options = null;
    if (includeSimilarity != null || includeSortVector != null) {
      options =
          new FindOneCommand.Options(
              includeSimilarity != null && includeSimilarity,
              includeSortVector != null && includeSortVector);
    }

    var command = new FindOneCommand(filterDef, projectionNode, sortDef, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Find multiple documents in a collection matching a filter")
  public Uni<String> find(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter, e.g. {\"status\": \"active\"}") @Nullable
          String filter,
      @ToolArg(description = "JSON object projection, e.g. {\"name\": 1}") @Nullable
          String projection,
      @ToolArg(description = "JSON object sort, e.g. {\"createdAt\": -1}") @Nullable String sort,
      @ToolArg(description = "Maximum number of documents to return") @Nullable Integer limit,
      @ToolArg(description = "Number of documents to skip") @Nullable Integer skip,
      @ToolArg(description = "Page state for pagination") @Nullable String pageState,
      @ToolArg(description = "Include similarity score in response") @Nullable
          Boolean includeSimilarity,
      @ToolArg(description = "Return vector embedding used for ANN sorting") @Nullable
          Boolean includeSortVector) {

    var filterDef = helper.toFilterDefinition(filter);
    var projectionNode = helper.parseJsonNode(projection);
    var sortDef = helper.toSortDefinition(sort);

    FindCommand.Options options = null;
    if (limit != null
        || skip != null
        || pageState != null
        || includeSimilarity != null
        || includeSortVector != null) {
      options =
          new FindCommand.Options(
              limit,
              skip,
              pageState,
              includeSimilarity != null && includeSimilarity,
              includeSortVector != null && includeSortVector);
    }

    var command = new FindCommand(filterDef, projectionNode, sortDef, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Count the number of documents in a collection matching a filter")
  public Uni<String> countDocuments(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter, e.g. {\"status\": \"active\"}") @Nullable
          String filter) {

    var filterDef = helper.toFilterDefinition(filter);
    var command = new CountDocumentsCommand(filterDef);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Get an estimated count of all documents in a collection (fast, approximate)")
  public Uni<String> estimatedDocumentCount(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection) {

    var command = new EstimatedDocumentCountCommand();
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  // ---- Document Write Operations ----

  @Tool(description = "Insert a single document into a collection")
  public Uni<String> insertOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON document to insert, e.g. {\"name\": \"John\", \"age\": 30}")
          String document) {

    JsonNode docNode = helper.parseJsonNode(document);
    var command = new InsertOneCommand(docNode);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Insert multiple documents into a collection")
  public Uni<String> insertMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(
              description =
                  "JSON array of documents to insert, e.g. [{\"name\": \"John\"}, {\"name\": \"Jane\"}]")
          String documents,
      @ToolArg(description = "When true, insert documents sequentially and fail on first error")
          @Nullable
          Boolean ordered,
      @ToolArg(description = "When true, return individual document response with status") @Nullable
          Boolean returnDocumentResponses) {

    JsonNode docsNode = helper.parseJsonNode(documents);
    List<JsonNode> docList =
        StreamSupport.stream(docsNode.spliterator(), false)
            .collect(java.util.stream.Collectors.toList());

    InsertManyCommand.Options options = null;
    if (ordered != null || returnDocumentResponses != null) {
      options =
          new InsertManyCommand.Options(
              ordered != null && ordered,
              returnDocumentResponses != null && returnDocumentResponses);
    }

    var command = new InsertManyCommand(docList, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  // ---- Document Delete Operations ----

  @Tool(description = "Delete a single document from a collection matching a filter")
  public Uni<String> deleteOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify the document to delete") String filter,
      @ToolArg(description = "JSON object sort to determine which document to delete") @Nullable
          String sort) {

    var filterDef = helper.toFilterDefinition(filter);
    var sortDef = helper.toSortDefinition(sort);
    var command = new DeleteOneCommand(filterDef, sortDef);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Delete multiple documents from a collection matching a filter")
  public Uni<String> deleteMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify documents to delete") @Nullable
          String filter) {

    var filterDef = helper.toFilterDefinition(filter);
    var command = new DeleteManyCommand(filterDef);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  // ---- Document Update Operations ----

  @Tool(description = "Update a single document in a collection matching a filter")
  public Uni<String> updateOne(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify the document") @Nullable String filter,
      @ToolArg(
              description =
                  "JSON update clause, e.g. {\"$set\": {\"name\": \"NewName\"}, \"$inc\": {\"count\": 1}}")
          String update,
      @ToolArg(description = "JSON object sort to determine which document to update") @Nullable
          String sort,
      @ToolArg(description = "When true, create a new document if no match is found") @Nullable
          Boolean upsert) {

    var filterDef = helper.toFilterDefinition(filter);
    var updateClause = helper.toUpdateClause(update);
    var sortDef = helper.toSortDefinition(sort);

    UpdateOneCommand.Options options = null;
    if (upsert != null) {
      options = new UpdateOneCommand.Options(upsert);
    }

    var command = new UpdateOneCommand(filterDef, updateClause, sortDef, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Update multiple documents in a collection matching a filter")
  public Uni<String> updateMany(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify documents") @Nullable String filter,
      @ToolArg(description = "JSON update clause, e.g. {\"$set\": {\"status\": \"archived\"}}")
          String update,
      @ToolArg(description = "When true, create a new document if no match is found") @Nullable
          Boolean upsert,
      @ToolArg(description = "Page state for continued updates") @Nullable String pageState) {

    var filterDef = helper.toFilterDefinition(filter);
    var updateClause = helper.toUpdateClause(update);

    UpdateManyCommand.Options options = null;
    if (upsert != null || pageState != null) {
      options = new UpdateManyCommand.Options(upsert != null && upsert, pageState);
    }

    var command = new UpdateManyCommand(filterDef, updateClause, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  // ---- Find-and-Modify Operations ----

  @Tool(
      description =
          "Find a single document matching a filter and delete it, returning the deleted document")
  public Uni<String> findOneAndDelete(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify the document") @Nullable String filter,
      @ToolArg(description = "JSON object sort to determine which document to delete") @Nullable
          String sort,
      @ToolArg(description = "JSON object projection for the returned document") @Nullable
          String projection) {

    var filterDef = helper.toFilterDefinition(filter);
    var sortDef = helper.toSortDefinition(sort);
    var projectionNode = helper.parseJsonNode(projection);

    var command = new FindOneAndDeleteCommand(filterDef, sortDef, projectionNode);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Find a single document matching a filter and replace it with a new document")
  public Uni<String> findOneAndReplace(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify the document") @Nullable String filter,
      @ToolArg(description = "JSON object sort to determine which document to replace") @Nullable
          String sort,
      @ToolArg(description = "JSON object projection for the returned document") @Nullable
          String projection,
      @ToolArg(description = "JSON replacement document") String replacement,
      @ToolArg(description = "'before' or 'after' - which version of document to return") @Nullable
          String returnDocument,
      @ToolArg(description = "When true, create a new document if no match is found") @Nullable
          Boolean upsert) {

    var filterDef = helper.toFilterDefinition(filter);
    var sortDef = helper.toSortDefinition(sort);
    var projectionNode = helper.parseJsonNode(projection);
    var replacementNode = (ObjectNode) helper.parseJsonNode(replacement);

    FindOneAndReplaceCommand.Options options = null;
    if (returnDocument != null || upsert != null) {
      options = new FindOneAndReplaceCommand.Options(returnDocument, upsert != null && upsert);
    }

    var command =
        new FindOneAndReplaceCommand(filterDef, sortDef, projectionNode, replacementNode, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Find a single document matching a filter and update it")
  public Uni<String> findOneAndUpdate(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the collection") String collection,
      @ToolArg(description = "JSON object filter to identify the document") @Nullable String filter,
      @ToolArg(description = "JSON object projection for the returned document") @Nullable
          String projection,
      @ToolArg(description = "JSON object sort to determine which document to update") @Nullable
          String sort,
      @ToolArg(description = "JSON update clause, e.g. {\"$set\": {\"name\": \"NewName\"}}")
          String update,
      @ToolArg(description = "'before' or 'after' - which version of document to return") @Nullable
          String returnDocument,
      @ToolArg(description = "When true, create a new document if no match is found") @Nullable
          Boolean upsert) {

    var filterDef = helper.toFilterDefinition(filter);
    var projectionNode = helper.parseJsonNode(projection);
    var sortDef = helper.toSortDefinition(sort);
    var updateClause = helper.toUpdateClause(update);

    FindOneAndUpdateCommand.Options options = null;
    if (returnDocument != null || upsert != null) {
      options = new FindOneAndUpdateCommand.Options(returnDocument, upsert != null && upsert);
    }

    var command =
        new FindOneAndUpdateCommand(filterDef, projectionNode, sortDef, updateClause, options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  // ---- Table DDL Operations (executed at collection-level endpoint) ----

  @Tool(description = "Alter a table's column definition (add/drop columns or vectorize settings)")
  public Uni<String> alterTable(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String collection,
      @ToolArg(
              description =
                  "JSON object of the full alterTable command body, e.g. {\"operation\": {\"add\": {\"columns\": {\"newcol\": {\"type\": \"text\"}}}}}")
          String commandJson) {

    var command = helper.deserializeCommand(commandJson, AlterTableCommand.class);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Create a regular index on a table column")
  public Uni<String> createIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String collection,
      @ToolArg(
              description =
                  "JSON object of the full createIndex command body, e.g. {\"name\": \"idx_name\", \"definition\": {\"column\": \"colname\"}, \"options\": {\"ifNotExists\": true}}")
          String commandJson) {

    var command = helper.deserializeCommand(commandJson, CreateIndexCommand.class);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Create a vector index on a table column for vector search sorting")
  public Uni<String> createVectorIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String collection,
      @ToolArg(
              description =
                  "JSON object of the full createVectorIndex command body, e.g. {\"name\": \"vec_idx\", \"definition\": {\"column\": \"embedding\", \"options\": {\"metric\": \"cosine\"}}}")
          String commandJson) {

    var command = helper.deserializeCommand(commandJson, CreateVectorIndexCommand.class);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "Create a text index on a table column for lexical filtering and sorting")
  public Uni<String> createTextIndex(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String collection,
      @ToolArg(
              description =
                  "JSON object of the full createTextIndex command body, e.g. {\"name\": \"txt_idx\", \"definition\": {\"column\": \"description\"}}")
          String commandJson) {

    var command = helper.deserializeCommand(commandJson, CreateTextIndexCommand.class);
    return helper.processCollectionCommand(keyspace, collection, command);
  }

  @Tool(description = "List all indexes on a table")
  public Uni<String> listIndexes(
      @ToolArg(description = "Name of the keyspace") String keyspace,
      @ToolArg(description = "Name of the table") String collection,
      @ToolArg(description = "If true, include index configuration details") @Nullable
          Boolean explain) {

    ListIndexesCommand.Options options = null;
    if (explain != null) {
      options = new ListIndexesCommand.Options(explain);
    }

    var command = new ListIndexesCommand(options);
    return helper.processCollectionCommand(keyspace, collection, command);
  }
}
