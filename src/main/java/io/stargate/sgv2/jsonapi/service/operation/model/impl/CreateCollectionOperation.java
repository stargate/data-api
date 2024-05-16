package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record CreateCollectionOperation(
    CommandContext commandContext,
    DatabaseLimitsConfig dbLimitsConfig,
    ObjectMapper objectMapper,
    CQLSessionCache cqlSessionCache,
    String name,
    boolean vectorSearch,
    int vectorSize,
    String vectorFunction,
    String comment,
    int ddlDelayMillis,
    boolean tooManyIndexesRollbackEnabled,
    // if true, deny all indexing option is set and no indexes will be created
    boolean indexingDenyAll)
    implements Operation {
  private static final Logger logger = LoggerFactory.getLogger(CreateCollectionOperation.class);

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
      String comment,
      int ddlDelayMillis,
      boolean tooManyIndexesRollbackEnabled,
      boolean indexingDenyAll) {
    return new CreateCollectionOperation(
        commandContext,
        dbLimitsConfig,
        objectMapper,
        cqlSessionCache,
        name,
        true,
        vectorSize,
        vectorFunction,
        comment,
        ddlDelayMillis,
        tooManyIndexesRollbackEnabled,
        indexingDenyAll);
  }

  public static CreateCollectionOperation withoutVectorSearch(
      CommandContext commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      String name,
      String comment,
      int ddlDelayMillis,
      boolean tooManyIndexesRollbackEnabled,
      boolean indexingDenyAll) {
    return new CreateCollectionOperation(
        commandContext,
        dbLimitsConfig,
        objectMapper,
        cqlSessionCache,
        name,
        false,
        0,
        null,
        comment,
        ddlDelayMillis,
        tooManyIndexesRollbackEnabled,
        indexingDenyAll);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    logger.info("Executing CreateCollectionOperation for {}", name);
    // validate Data API collection limit guardrail and get tableMetadata
    Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces =
        cqlSessionCache.getSession(dataApiRequestInfo).getMetadata().getKeyspaces();
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

    // if table doesn't exist, continue to create collection
    if (table == null) {
      return executeCollectionCreation(dataApiRequestInfo, queryExecutor, false);
    }
    // if table exists, compare existedCollectionSettings and newCollectionSettings
    CollectionSettings existedCollectionSettings =
        CollectionSettings.getCollectionSettings(table, objectMapper);
    CollectionSettings newCollectionSettings =
        CollectionSettings.getCollectionSettings(
            name,
            vectorSearch,
            vectorSize,
            CollectionSettings.SimilarityFunction.fromString(vectorFunction),
            comment,
            objectMapper);
    // if table exists we have to choices:
    // (1) trying to create with same options -> ok, proceed
    // (2) trying to create with different options -> error out
    if (existedCollectionSettings.equals(newCollectionSettings)) {
      return executeCollectionCreation(dataApiRequestInfo, queryExecutor, true);
    }
    return Uni.createFrom()
        .failure(
            ErrorCode.INVALID_COLLECTION_NAME.toApiException(
                "provided collection ('%s') already exists with different collection options",
                name));
  }

  /**
   * execute collection creation and indexes creation
   *
   * @param dataApiRequestInfo DataApiRequestInfo
   * @param queryExecutor QueryExecutor instance
   * @param collectionExisted boolean that says if collection existed before
   * @return Uni<Supplier<CommandResult>>
   */
  private Uni<Supplier<CommandResult>> executeCollectionCreation(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      boolean collectionExisted) {
    final Uni<AsyncResultSet> execute =
        queryExecutor.executeCreateSchemaChange(
            dataApiRequestInfo, getCreateTable(commandContext.namespace(), name));
    final Uni<Boolean> indexResult =
        execute
            .onItem()
            .delayIt()
            .by(Duration.ofMillis(ddlDelayMillis > 0 ? ddlDelayMillis : 100))
            .onItem()
            .transformToUni(
                res -> {
                  if (res.wasApplied()) {
                    final List<SimpleStatement> indexStatements =
                        getIndexStatements(commandContext.namespace(), name);
                    Multi<AsyncResultSet> indexResultMulti;
                    /*
                    CI will override ddlDelayMillis to 0 using `-Dstargate.jsonapi.operations.database-config.ddl-delay-millis=0`
                       to speed up the test execution
                       This is ok because CI is run as single cassandra instance and there is no need to wait for the schema changes to propagate
                    */

                    if (ddlDelayMillis == 0) {
                      indexResultMulti =
                          createIndexParallel(queryExecutor, dataApiRequestInfo, indexStatements);
                    } else {
                      indexResultMulti =
                          createIndexOrdered(queryExecutor, dataApiRequestInfo, indexStatements);
                    }

                    return indexResultMulti
                        .collect()
                        .asList()
                        .onItem()
                        .transform(
                            results -> {
                              final Optional<AsyncResultSet> first =
                                  results.stream()
                                      .filter(indexRes -> !(indexRes.wasApplied()))
                                      .findFirst();
                              return !first.isPresent();
                            });
                  } else {
                    return Uni.createFrom().item(false);
                  }
                });
    return indexResult
        .onItem()
        .transform(
            res -> {
              if (!res) {
                // table creation failure or index creation failure
                return ErrorCode.COLLECTION_CREATION_ERROR.toApiException(
                    "provided collection ('%s')", name);
              } else {
                return new SchemaChangeResult(true);
              }
            })
        .onFailure(
            error ->
                // InvalidQueryException(DB index limit violation)
                error instanceof InvalidQueryException
                    && error
                        .getMessage()
                        .matches(
                            ".*Cannot have more than \\d+ indexes, failed to create index on table.*"))
        .recoverWithUni(
            error -> {
              // if index creation violates DB index limit and collection not existed before,
              // and rollback is enabled, then drop the collection
              if (!collectionExisted && tooManyIndexesRollbackEnabled) {
                return cleanUpCollectionFailedWithTooManyIndex(dataApiRequestInfo, queryExecutor);
              }
              // if index creation violates DB index limit and collection existed before,
              // will not drop the collection
              return Uni.createFrom()
                  .item(
                      () ->
                          ErrorCode.TOO_MANY_INDEXES.toApiException(
                              "collection \"%s\" creation failed due to index creation failing; need %d indexes to create the collection;",
                              name, dbLimitsConfig.indexesNeededPerCollection()));
            });
  }

  /**
   * Create indexes for collections in ordered. This is to avoid schema change conflicts.
   *
   * @param queryExecutor
   * @param dataApiRequestInfo
   * @param indexStatements
   * @return
   */
  private Multi<AsyncResultSet> createIndexOrdered(
      QueryExecutor queryExecutor,
      DataApiRequestInfo dataApiRequestInfo,
      List<SimpleStatement> indexStatements) {
    return Multi.createFrom()
        .items(indexStatements.stream())
        .onItem()
        .transformToUni(
            indexStatement ->
                queryExecutor.executeCreateSchemaChange(dataApiRequestInfo, indexStatement))
        .concatenate();
  }

  /**
   * Create indexes for collections in parallel. TO speed up the CI actions.
   *
   * @param queryExecutor
   * @param dataApiRequestInfo
   * @param indexStatements
   * @return
   */
  private Multi<AsyncResultSet> createIndexParallel(
      QueryExecutor queryExecutor,
      DataApiRequestInfo dataApiRequestInfo,
      List<SimpleStatement> indexStatements) {
    return Multi.createFrom()
        .items(indexStatements.stream())
        .onItem()
        .transformToUni(
            indexStatement ->
                queryExecutor.executeCreateSchemaChange(dataApiRequestInfo, indexStatement))
        .merge();
  }

  public Uni<JsonApiException> cleanUpCollectionFailedWithTooManyIndex(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    DeleteCollectionOperation deleteCollectionOperation =
        new DeleteCollectionOperation(commandContext, name);
    return deleteCollectionOperation
        .execute(dataApiRequestInfo, queryExecutor)
        .onItem()
        .transform(
            res ->
                ErrorCode.TOO_MANY_INDEXES.toApiException(
                    "collection \"%s\" creation failed due to index creation failing; need %d indexes to create the collection;",
                    name, dbLimitsConfig.indexesNeededPerCollection()))
        .onFailure()
        .recoverWithItem(
            e -> {
              // This is unlikely to happen for delete collection though
              // Also return with TOO_MANY_INDEXES exception
              return ErrorCode.TOO_MANY_INDEXES.toApiException(
                  "collection \"%s\" creation failed due to index creation failing; need %d indexes to create the collection;",
                  name, dbLimitsConfig.indexesNeededPerCollection());
            });
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
            .toList();
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
      throw ErrorCode.TOO_MANY_INDEXES.toApiException(
          "cannot create a new collection; need %d indexes to create the collection; %d indexes already created in database, maximum %d",
          dbLimitsConfig.indexesNeededPerCollection(),
          saisUsed,
          dbLimitsConfig.indexesAvailablePerDatabase());
    }

    return null;
  }

  public SimpleStatement getCreateTable(String keyspace, String table) {
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
      if (comment != null) {
        createTableWithVector = createTableWithVector + " WITH comment = '" + comment + "'";
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
      if (comment != null) {
        createTable = createTable + " WITH comment = '" + comment + "'";
      }
      return SimpleStatement.newInstance(String.format(createTable, keyspace, table));
    }
  }

  public List<SimpleStatement> getIndexStatements(String keyspace, String table) {
    List<SimpleStatement> statements = new ArrayList<>(10);
    if (!indexingDenyAll()) {
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
    }

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
