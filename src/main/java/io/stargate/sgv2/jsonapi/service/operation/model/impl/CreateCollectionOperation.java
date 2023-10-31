package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public record CreateCollectionOperation(
    CommandContext commandContext,
    DatabaseLimitsConfig dbLimitsConfig,
    ObjectMapper objectMapper,
    SchemaManager schemaManager,
    String name,
    boolean vectorSearch,
    int vectorSize,
    String vectorFunction,
    String vectorize)
    implements Operation {

  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE_FUNCTION =
          keyspace -> {
            String message =
                "INVALID_ARGUMENT: Unknown namespace '%s', you must create it first."
                    .formatted(keyspace);
            Exception exception = new JsonApiException(ErrorCode.NAMESPACE_DOES_NOT_EXIST, message);
            return Uni.createFrom().failure(exception);
          };

  public static CreateCollectionOperation withVectorSearch(
      CommandContext commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
      ObjectMapper objectMapper,
      SchemaManager schemaManager,
      String name,
      int vectorSize,
      String vectorFunction,
      String vectorize) {
    return new CreateCollectionOperation(
        commandContext,
        dbLimitsConfig,
        objectMapper,
        schemaManager,
        name,
        true,
        vectorSize,
        vectorFunction,
        vectorize);
  }

  public static CreateCollectionOperation withoutVectorSearch(
      CommandContext commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
      ObjectMapper objectMapper,
      SchemaManager schemaManager,
      String name) {
    return new CreateCollectionOperation(
        commandContext, dbLimitsConfig, objectMapper, schemaManager, name, false, 0, null, null);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    return schemaManager
        .getTables(commandContext.namespace(), MISSING_KEYSPACE_FUNCTION)
        .collect()
        .asList()
        .map(tables -> findTableAndValidateLimits(tables, name))
        .onItem()
        .transformToUni(
            table -> {
              // table doesn't exist
              if (table == null) {
                return executeCollectionCreation(queryExecutor);
              }
              // get collection settings from the existing collection
              CollectionSettings collectionSettings =
                  CollectionSettings.getCollectionSettings(table, objectMapper);
              // get collection settings from user input
              CollectionSettings collectionSettings_cur =
                  CollectionSettings.getCollectionSettings(
                      name,
                      vectorSearch,
                      vectorSize,
                      CollectionSettings.SimilarityFunction.fromString(vectorFunction),
                      vectorize,
                      objectMapper);
              // if table exists and user want to create a vector collection with the same name
              if (vectorSearch) {
                // if existing collection is a vector collection
                if (collectionSettings.vectorEnabled()) {
                  if (collectionSettings.equals(collectionSettings_cur)) {
                    // if settings are equal, no error
                    return executeCollectionCreation(queryExecutor);
                  } else {
                    // if settings are not equal, error out
                    return Uni.createFrom()
                        .failure(
                            new JsonApiException(
                                ErrorCode.INVALID_COLLECTION_NAME,
                                "The provided collection name '%s' already exists with a different vector setting."
                                    .formatted(name)));
                  }
                } else {
                  // if existing collection is a non-vector collection, error out
                  return Uni.createFrom()
                      .failure(
                          new JsonApiException(
                              ErrorCode.INVALID_COLLECTION_NAME,
                              "The provided collection name '%s' already exists with a non-vector setting."
                                  .formatted(name)));
                }
              } else { // if table exists and user want to create a non-vector collection
                // if existing table is vector enabled, error out
                if (collectionSettings.vectorEnabled()) {
                  return Uni.createFrom()
                      .failure(
                          new JsonApiException(
                              ErrorCode.INVALID_COLLECTION_NAME,
                              "The provided collection name '%s' already exists with a vector setting."
                                  .formatted(name)));
                } else {
                  // if existing table is a non-vector collection, continue
                  return executeCollectionCreation(queryExecutor);
                }
              }
            });
  }

  private Uni<Supplier<CommandResult>> executeCollectionCreation(QueryExecutor queryExecutor) {
    final Uni<QueryOuterClass.ResultSet> execute =
        queryExecutor.executeSchemaChange(getCreateTable(commandContext.namespace(), name));
    final Uni<Boolean> indexResult =
        execute
            .onItem()
            .transformToUni(
                res -> {
                  final List<QueryOuterClass.Query> indexStatements =
                      getIndexStatements(commandContext.namespace(), name);
                  List<Uni<QueryOuterClass.ResultSet>> indexes = new ArrayList<>(10);
                  indexStatements.stream()
                      .forEach(index -> indexes.add(queryExecutor.executeSchemaChange(index)));
                  return Uni.combine().all().unis(indexes).combinedWith(results -> true);
                });
    return indexResult.onItem().transform(res -> new SchemaChangeResult(res));
  }

  /**
   * Method for finding existing table with given name, if one exists and returning that table; or
   * if not, verify maximum table limit and return null.
   *
   * @return Existing table with given name, if any; {@code null} if not
   */
  Schema.CqlTable findTableAndValidateLimits(List<Schema.CqlTable> tables, String name) {
    for (Schema.CqlTable table : tables) {
      if (table.getName().equals(name)) {
        return table;
      }
    }
    final int MAX_COLLECTIONS = dbLimitsConfig.maxCollections();
    if (tables.size() >= MAX_COLLECTIONS) {
      throw new JsonApiException(
          ErrorCode.TOO_MANY_COLLECTIONS,
          String.format(
              "%s: number of collections in database cannot exceed %d, already have %d",
              ErrorCode.TOO_MANY_COLLECTIONS.getMessage(), MAX_COLLECTIONS, tables.size()));
    }
    return null;
  }

  protected QueryOuterClass.Query getCreateTable(String keyspace, String table) {
    if (vectorSearch) {
      String createTableWithVector =
          "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ("
              + "    key                 tuple<tinyint,text>,"
              + "    tx_id               timeuuid, "
              + "    doc_json            text,"
              + "    exist_keys          set<text>,"
              + "    array_size          map<text, int>,"
              + "    array_contains      set<text>,"
              + "    query_bool_values   map<text, tinyint>,"
              + "    query_dbl_values    map<text, decimal>,"
              + "    query_text_values   map<text, text>, "
              + "    query_timestamp_values map<text, timestamp>, "
              + "    query_null_values   set<text>,     "
              + "    query_vector_value  VECTOR<FLOAT, "
              + vectorSize
              + ">, "
              + "    PRIMARY KEY (key))";
      if (vectorize != null) {
        createTableWithVector = createTableWithVector + " WITH comment = '" + vectorize + "'";
      }
      return QueryOuterClass.Query.newBuilder()
          .setCql(String.format(createTableWithVector, keyspace, table))
          .build();
    } else {
      String createTable =
          "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ("
              + "    key                 tuple<tinyint,text>,"
              + "    tx_id               timeuuid, "
              + "    doc_json            text,"
              + "    exist_keys          set<text>,"
              + "    array_size          map<text, int>,"
              + "    array_contains      set<text>,"
              + "    query_bool_values   map<text, tinyint>,"
              + "    query_dbl_values    map<text, decimal>,"
              + "    query_text_values   map<text, text>, "
              + "    query_timestamp_values map<text, timestamp>, "
              + "    query_null_values   set<text>, "
              + "    PRIMARY KEY (key))";

      return QueryOuterClass.Query.newBuilder()
          .setCql(String.format(createTable, keyspace, table))
          .build();
    }
  }

  protected List<QueryOuterClass.Query> getIndexStatements(String keyspace, String table) {
    List<QueryOuterClass.Query> statements = new ArrayList<>(10);

    String existKeys =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON \"%s\".\"%s\" (exist_keys) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(existKeys, table, keyspace, table))
            .build());

    String arraySize =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON \"%s\".\"%s\" (entries(array_size)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(arraySize, table, keyspace, table))
            .build());

    String arrayContains =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON \"%s\".\"%s\" (array_contains) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(arrayContains, table, keyspace, table))
            .build());

    String boolQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON \"%s\".\"%s\" (entries(query_bool_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(boolQuery, table, keyspace, table))
            .build());

    String dblQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON \"%s\".\"%s\" (entries(query_dbl_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(dblQuery, table, keyspace, table))
            .build());

    String textQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON \"%s\".\"%s\" (entries(query_text_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(textQuery, table, keyspace, table))
            .build());

    String timestampQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_timestamp_values ON \"%s\".\"%s\" (entries(query_timestamp_values)) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(timestampQuery, table, keyspace, table))
            .build());

    String nullQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON \"%s\".\"%s\" (query_null_values) USING 'StorageAttachedIndex'";
    statements.add(
        QueryOuterClass.Query.newBuilder()
            .setCql(String.format(nullQuery, table, keyspace, table))
            .build());

    if (vectorSearch) {
      String vectorSearch =
          "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_vector_value ON \"%s\".\"%s\" (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': '"
              + vectorFunction()
              + "'}";
      statements.add(
          QueryOuterClass.Query.newBuilder()
              .setCql(String.format(vectorSearch, table, keyspace, table))
              .build());
    }
    return statements;
  }
}
