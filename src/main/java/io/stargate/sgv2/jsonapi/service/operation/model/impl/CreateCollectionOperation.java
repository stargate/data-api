package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record CreateCollectionOperation(
    CommandContext commandContext,
    DatabaseLimitsConfig dbLimitsConfig,
    ObjectMapper objectMapper,
    CQLSessionCache cqlSessionCache,
    String name,
    boolean vectorSearch,
    int vectorSize,
    String vectorFunction,
    String vectorize)
    implements Operation {
  // shared matcher instance used to tell Collections from Tables
  private static final JsonapiTableMatcher COLLECTION_MATCHER = new JsonapiTableMatcher();

  public static CreateCollectionOperation withVectorSearch(
      CommandContext commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      String name,
      int vectorSize,
      String vectorFunction,
      String vectorize) {
    return new CreateCollectionOperation(
        commandContext,
        dbLimitsConfig,
        objectMapper,
        cqlSessionCache,
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
      CQLSessionCache cqlSessionCache,
      String name) {
    return new CreateCollectionOperation(
        commandContext, dbLimitsConfig, objectMapper, cqlSessionCache, name, false, 0, null, null);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces =
        cqlSessionCache.getSession().getMetadata().getKeyspaces();
    KeyspaceMetadata currKeyspace =
        allKeyspaces.get(CqlIdentifier.fromInternal(commandContext.namespace()));
    if (currKeyspace == null) {
      return Uni.createFrom()
          .failure(
              new JsonApiException(
                  ErrorCode.NAMESPACE_DOES_NOT_EXIST,
                  "INVALID_ARGUMENT: Unknown namespace '%s', you must create it first."
                      .formatted(commandContext.namespace())));
    }
    TableMetadata table = findTableAndValidateLimits(allKeyspaces, currKeyspace, name);

    // table doesn't exist, continue
    if (table == null) {
      return executeCollectionCreation(queryExecutor);
    }
    // if table exist:
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
  }

  private Uni<Supplier<CommandResult>> executeCollectionCreation(QueryExecutor queryExecutor) {
    final Uni<AsyncResultSet> execute =
        queryExecutor.executeSchemaChange(getCreateTable(commandContext.namespace(), name));
    final Uni<Boolean> indexResult =
        execute
            .onItem()
            .transformToUni(
                res -> {
                  if (res.wasApplied()) {
                    final List<SimpleStatement> indexStatements =
                        getIndexStatements(commandContext.namespace(), name);
                    List<Uni<AsyncResultSet>> indexes = new ArrayList<>(10);
                    indexStatements.stream()
                        .forEach(index -> indexes.add(queryExecutor.executeSchemaChange(index)));
                    return Uni.combine()
                        .all()
                        .unis(indexes)
                        .combinedWith(
                            results -> {
                              final Optional<?> first =
                                  results.stream()
                                      .filter(
                                          indexRes -> !(((AsyncResultSet) indexRes).wasApplied()))
                                      .findFirst();
                              return first.isPresent() ? false : true;
                            });
                  } else {
                    return Uni.createFrom().item(false);
                  }
                });
    return indexResult.onItem().transform(SchemaChangeResult::new);
  }

  /**
   * Method for finding existing table with given name, if one exists and returning that table; or
   * if not, verify maximum table limit and return null.
   *
   * @return Existing table with given name, if any; {@code null} if not
   */
  TableMetadata findTableAndValidateLimits(
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces,
      KeyspaceMetadata currKeyspace,
      String tableName) {
    // First: do we already have a Table with the same name? If so, return it
    for (TableMetadata table : currKeyspace.getTables().values()) {
      if (table.getName().asInternal().equals(tableName)) {
        return table;
      }
    }
    // Otherwise we need to check if we can create a new Collection based on limits;
    // limits are calculated across the whole Database, so all Keyspaces need to be checked.
    final List<TableMetadata> allTables =
        allKeyspaces.values().stream()
            .map(keyspace -> keyspace.getTables().values())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    final long collectionCount = allTables.stream().filter(COLLECTION_MATCHER).count();
    final int MAX_COLLECTIONS = dbLimitsConfig.maxCollections();
    if (collectionCount >= MAX_COLLECTIONS) {
      throw new JsonApiException(
          ErrorCode.TOO_MANY_COLLECTIONS,
          String.format(
              "%s: number of collections in database cannot exceed %d, already have %d",
              ErrorCode.TOO_MANY_COLLECTIONS.getMessage(), MAX_COLLECTIONS, collectionCount));
    }
    // And then see how many Indexes have been created, how many available
    int saisUsed = allTables.stream().mapToInt(table -> table.getIndexes().size()).sum();
    if ((saisUsed + dbLimitsConfig.indexesNeededPerCollection())
        > dbLimitsConfig.indexesAvailablePerDatabase()) {
      throw new JsonApiException(
          ErrorCode.TOO_MANY_INDEXES,
          String.format(
              "%s: cannot create a new collection; %d indexes already created in database, maximum %d",
              ErrorCode.TOO_MANY_INDEXES.getMessage(),
              saisUsed,
              dbLimitsConfig.indexesAvailablePerDatabase()));
    }

    return null;
  }

  protected SimpleStatement getCreateTable(String keyspace, String table) {
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
      return SimpleStatement.newInstance(String.format(createTableWithVector, keyspace, table));
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

      return SimpleStatement.newInstance(String.format(createTable, keyspace, table));
    }
  }

  protected List<SimpleStatement> getIndexStatements(String keyspace, String table) {
    List<SimpleStatement> statements = new ArrayList<>(10);

    String existKeys =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_exists_keys ON \"%s\".\"%s\" (exist_keys) USING 'StorageAttachedIndex'";

    statements.add(SimpleStatement.newInstance(String.format(existKeys, table, keyspace, table)));

    String arraySize =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_size ON \"%s\".\"%s\" (entries(array_size)) USING 'StorageAttachedIndex'";
    statements.add(SimpleStatement.newInstance(String.format(arraySize, table, keyspace, table)));

    String arrayContains =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_array_contains ON \"%s\".\"%s\" (array_contains) USING 'StorageAttachedIndex'";
    statements.add(
        SimpleStatement.newInstance(String.format(arrayContains, table, keyspace, table)));

    String boolQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_bool_values ON \"%s\".\"%s\" (entries(query_bool_values)) USING 'StorageAttachedIndex'";
    statements.add(SimpleStatement.newInstance(String.format(boolQuery, table, keyspace, table)));

    String dblQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_dbl_values ON \"%s\".\"%s\" (entries(query_dbl_values)) USING 'StorageAttachedIndex'";
    statements.add(SimpleStatement.newInstance(String.format(dblQuery, table, keyspace, table)));

    String textQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_text_values ON \"%s\".\"%s\" (entries(query_text_values)) USING 'StorageAttachedIndex'";
    statements.add(SimpleStatement.newInstance(String.format(textQuery, table, keyspace, table)));

    String timestampQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_timestamp_values ON \"%s\".\"%s\" (entries(query_timestamp_values)) USING 'StorageAttachedIndex'";
    statements.add(
        SimpleStatement.newInstance(String.format(timestampQuery, table, keyspace, table)));

    String nullQuery =
        "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_null_values ON \"%s\".\"%s\" (query_null_values) USING 'StorageAttachedIndex'";
    statements.add(SimpleStatement.newInstance(String.format(nullQuery, table, keyspace, table)));

    if (vectorSearch) {
      String vectorSearch =
          "CREATE CUSTOM INDEX IF NOT EXISTS %s_query_vector_value ON \"%s\".\"%s\" (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': '"
              + vectorFunction()
              + "'}";
      statements.add(
          SimpleStatement.newInstance(String.format(vectorSearch, table, keyspace, table)));
    }
    return statements;
  }
}
