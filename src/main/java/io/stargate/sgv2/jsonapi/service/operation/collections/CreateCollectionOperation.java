package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record CreateCollectionOperation(
    CommandContext<KeyspaceSchemaObject> commandContext,
    DatabaseLimitsConfig dbLimitsConfig,
    ObjectMapper objectMapper,
    CQLSessionCache cqlSessionCache,
    String name,
    boolean vectorSearch,
    int vectorSize,
    String vectorFunction,
    String sourceModel,
    String comment,
    int ddlDelayMillis,
    boolean tooManyIndexesRollbackEnabled,
    // if true, deny all indexing option is set and no indexes will be created
    boolean indexingDenyAll,
    CollectionLexicalConfig lexicalConfig,
    CollectionRerankDef rerankDef)
    implements Operation {
  private static final Logger logger = LoggerFactory.getLogger(CreateCollectionOperation.class);

  // shared matcher instance used to tell Collections from Tables
  private static final CollectionTableMatcher COLLECTION_MATCHER = new CollectionTableMatcher();

  public static CreateCollectionOperation withVectorSearch(
      CommandContext<KeyspaceSchemaObject> commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      String name,
      int vectorSize,
      String vectorFunction,
      String sourceModel,
      String comment,
      int ddlDelayMillis,
      boolean tooManyIndexesRollbackEnabled,
      boolean indexingDenyAll,
      CollectionLexicalConfig lexicalConfig,
      CollectionRerankDef rerankDef) {
    return new CreateCollectionOperation(
        commandContext,
        dbLimitsConfig,
        objectMapper,
        cqlSessionCache,
        name,
        true,
        vectorSize,
        vectorFunction,
        sourceModel,
        comment,
        ddlDelayMillis,
        tooManyIndexesRollbackEnabled,
        indexingDenyAll,
        Objects.requireNonNull(lexicalConfig),
        Objects.requireNonNull(rerankDef));
  }

  public static CreateCollectionOperation withoutVectorSearch(
      CommandContext<KeyspaceSchemaObject> commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      String name,
      String comment,
      int ddlDelayMillis,
      boolean tooManyIndexesRollbackEnabled,
      boolean indexingDenyAll,
      CollectionLexicalConfig lexicalConfig,
      CollectionRerankDef rerankDef) {
    return new CreateCollectionOperation(
        commandContext,
        dbLimitsConfig,
        objectMapper,
        cqlSessionCache,
        name,
        false,
        0,
        null,
        null,
        comment,
        ddlDelayMillis,
        tooManyIndexesRollbackEnabled,
        indexingDenyAll,
        Objects.requireNonNull(lexicalConfig),
        Objects.requireNonNull(rerankDef));
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    logger.info(
        "Executing CreateCollectionOperation for {}.{} with definition: {}",
        commandContext.schemaObject().identifier().keyspace(),
        name,
        comment);

    return queryExecutor
        .getDriverMetadata(requestContext)
        .map(Metadata::getKeyspaces)
        .flatMap(
            allKeyspaces -> {

              //  aaron - 23 may 2025, having this huge lambda is not great. This is a partial
              // refactor to make
              // this operation fully Async, without refactoring all the logic.
              KeyspaceMetadata currKeyspace =
                  allKeyspaces.get(commandContext.schemaObject().identifier().keyspace());

              if (currKeyspace == null) {
                return Uni.createFrom()
                    .failure(
                        SchemaException.Code.UNKNOWN_KEYSPACE.get(
                            errVars(commandContext.schemaObject())));
              }

              TableMetadata tableMetadata =
                  findTableAndValidateLimits(allKeyspaces, currKeyspace, name);

              // if table doesn't exist, continue to create collection
              if (tableMetadata == null) {
                return executeCollectionCreation(
                    requestContext, queryExecutor, lexicalConfig(), false);
              }

              // if table exists, compare existingCollectionSettings and newCollectionSettings
              CollectionSchemaObject existingCollectionSettings =
                  CollectionSchemaObject.getCollectionSettings(
                      requestContext.tenant(), tableMetadata, objectMapper);

              // Use the fromNameOrDefault() so if not specified it will default
              var embeddingSourceModel =
                  EmbeddingSourceModel.fromApiNameOrDefault(sourceModel)
                      .orElseThrow(
                          () -> EmbeddingSourceModel.getUnknownSourceModelException(sourceModel));

              var similarityFunction =
                  SimilarityFunction.fromApiNameOrDefault(vectorFunction)
                      .orElseThrow(
                          () -> SimilarityFunction.getUnknownFunctionException(vectorFunction));

              CollectionSchemaObject newCollectionSettings =
                  CollectionSchemaObject.getCollectionSettings(
                      requestContext.tenant(),
                      currKeyspace.getName().asInternal(),
                      name,
                      tableMetadata,
                      vectorSearch,
                      vectorSize,
                      similarityFunction,
                      embeddingSourceModel,
                      comment,
                      objectMapper);
              // If Collection exists we have a choice:
              // (1) trying to create with same options -> ok, proceed
              // (2) trying to create with different options -> error out
              // but before deciding (2), we need to consider one specific backwards-compatibility
              // case: that of existing pre-lexical/pre-reranking collection, being re-created
              // without definitions for lexical/pre-ranking. Although it would create a new
              // Collection with both enabled, it should NOT fail if attempted on an existing
              // Collection with pre-lexical/pre-reranking settings but silently succeed.

              boolean settingsAreEqual = existingCollectionSettings.equals(newCollectionSettings);

              if (!settingsAreEqual) {
                final var oldLexical = existingCollectionSettings.lexicalConfig();
                final var newLexical = lexicalConfig();
                final var oldReranking = existingCollectionSettings.rerankingConfig();
                final var newReranking = rerankDef();

                // So: for backwards compatibility reasons we may need to override settings if
                // (and only if) the collection was created before lexical and reranking.
                // In addition, we need to check that new lexical settings are for defaults
                // (difficult to check the same for reranking; for now assume that if lexical
                // is default, reranking is also default).
                if (oldLexical == CollectionLexicalConfig.configForPreLexical()
                    && newLexical == CollectionLexicalConfig.configForDefault()
                    && oldReranking == CollectionRerankDef.configForPreRerankingCollection()
                    && newReranking == CollectionRerankDef.configForDefault()) {
                  var originalNewSettings = newCollectionSettings;
                  newCollectionSettings =
                      newCollectionSettings.withLexicalAndRerankOverrides(
                          oldLexical, existingCollectionSettings.rerankingConfig());
                  // and now re-check if settings are the same
                  settingsAreEqual = existingCollectionSettings.equals(newCollectionSettings);
                  logger.info(
                      "CreateCollectionOperation for {}.{} with existing legacy lexical/reranking settings, new settings differ. Tried to unify, result: {}"
                          + " Old settings: {}, New settings: {}",
                      commandContext.schemaObject().identifier().keyspace(),
                      name,
                      settingsAreEqual,
                      existingCollectionSettings,
                      originalNewSettings);
                } else {
                  logger.info(
                      "CreateCollectionOperation for {}.{} with different settings (but not old legacy lexical/reranking settings), cannot unify."
                          + " Old settings: {}, New settings: {}",
                      commandContext.schemaObject().identifier().keyspace(),
                      name,
                      existingCollectionSettings,
                      newCollectionSettings);
                }
              }

              if (settingsAreEqual) {
                return executeCollectionCreation(
                    requestContext, queryExecutor, newCollectionSettings.lexicalConfig(), true);
              }
              return Uni.createFrom()
                  .failure(
                      ErrorCodeV1.EXISTING_COLLECTION_DIFFERENT_SETTINGS.toApiException(
                          "trying to create Collection ('%s') with different settings", name));
            });
  }

  /**
   * execute collection creation and indexes creation
   *
   * @param dataApiRequestInfo DBRequestContext
   * @param queryExecutor QueryExecutor instance
   * @param lexicalConfig Lexical configuration for the collection
   * @param collectionExisted boolean that says if collection existed before
   * @return Uni<Supplier<CommandResult>>
   */
  private Uni<Supplier<CommandResult>> executeCollectionCreation(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      CollectionLexicalConfig lexicalConfig,
      boolean collectionExisted) {
    final Uni<AsyncResultSet> execute =
        queryExecutor.executeCreateSchemaChange(
            dataApiRequestInfo,
            getCreateTable(
                commandContext.schemaObject().identifier().keyspace().asInternal(),
                name,
                vectorSearch,
                vectorSize,
                comment,
                lexicalConfig));
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
                        getIndexStatements(
                            commandContext.schemaObject().identifier().keyspace().asInternal(),
                            name,
                            lexicalConfig,
                            collectionExisted);
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
                return ErrorCodeV1.COLLECTION_CREATION_ERROR.toApiException(
                    "provided collection ('%s')", name);
              } else {
                return new SchemaChangeResult(true);
              }
            })
        .onFailure(
            error ->
                // InvalidQueryException(DB index limit violation)
                error instanceof InvalidQueryException
                    && (error
                            .getMessage()
                            .matches(
                                ".*Cannot have more than \\d+ indexes, failed to create index on table.*")
                        || error.getMessage().matches("Index .* already exists")))
        .recoverWithUni(
            // this block only handles the case where the index creation fails because of index
            // limit or already exists during create collection
            error -> {
              // if index creation fails and collection not existed before and rollback is enabled,
              // then drop the collection
              if (!collectionExisted && tooManyIndexesRollbackEnabled) {
                return cleanUpCollectionFailedWithTooManyIndex(dataApiRequestInfo, queryExecutor);
              }

              if (error.getMessage().matches("Index .* already exists")) {
                // if index creation fails because index already exists
                return Uni.createFrom()
                    .item(
                        () ->
                            ErrorCodeV1.INDEXES_CREATION_FAILED.toApiException(
                                "The index failed to create because an index with the collection name (%s) prefix already exists.",
                                name));
              } else {
                // if index creation violates DB index limit and collection existed before,
                // will not drop the collection
                return Uni.createFrom()
                    .item(
                        () ->
                            ErrorCodeV1.TOO_MANY_INDEXES.toApiException(
                                "Failed to create index for collection '%s': The number of required indexes exceeds the provisioned limit for the database.",
                                name));
              }
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
      RequestContext dataApiRequestInfo,
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
      RequestContext dataApiRequestInfo,
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
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {

    DeleteCollectionCollectionOperation deleteCollectionCollectionOperation =
        new DeleteCollectionCollectionOperation(commandContext, name);
    return deleteCollectionCollectionOperation
        .execute(dataApiRequestInfo, queryExecutor)
        .onItem()
        .transform(
            res ->
                ErrorCodeV1.TOO_MANY_INDEXES.toApiException(
                    "collection \"%s\" creation failed due to index creation failing; need %d indexes to create the collection;",
                    name, dbLimitsConfig.indexesNeededPerCollection()))
        .onFailure()
        .recoverWithItem(
            e -> {
              // This is unlikely to happen for delete collection though
              // Also return with TOO_MANY_INDEXES exception
              return ErrorCodeV1.TOO_MANY_INDEXES.toApiException(
                  "collection \"%s\" creation failed due to index creation failing; need %d indexes to create the collection;",
                  name, dbLimitsConfig.indexesNeededPerCollection());
            });
  }

  /**
   * Method for finding existing collection with given name: 1. if valid one exists and returning
   * that table; 2. if invalid one exists, error out; 3. if no table exists with given name, verify
   * maximum table limit and return null;
   *
   * @return Existing valid collection with given name, if any; {@code null} if not
   */
  TableMetadata findTableAndValidateLimits(
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces,
      KeyspaceMetadata currKeyspace,
      String tableName) {

    // First: do we already have a Table with the same name?
    for (TableMetadata table : currKeyspace.getTables().values()) {
      if (table.getName().asInternal().equals(tableName)) {
        // If that is not a valid Data API collection, error out the createCollectionCommand
        if (!COLLECTION_MATCHER.test(table)) {
          throw ErrorCodeV1.EXISTING_TABLE_NOT_DATA_API_COLLECTION.toApiException(
              "table ('%s') already exists and it is not a valid Data API Collection", tableName);
        }
        // If that is a valid Data API table, we returned it
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
      throw ErrorCodeV1.TOO_MANY_COLLECTIONS.toApiException(
          "number of collections in database cannot exceed %d, already have %d",
          MAX_COLLECTIONS, collectionCount);
    }

    // And then see how many Indexes have been created, how many available
    int saisUsed = allTables.stream().mapToInt(table -> table.getIndexes().size()).sum();
    if ((saisUsed + dbLimitsConfig.indexesNeededPerCollection())
        > dbLimitsConfig.indexesAvailablePerDatabase()) {
      throw ErrorCodeV1.TOO_MANY_INDEXES.toApiException(
          "cannot create a new collection; need %d indexes to create the collection; %d indexes already created in database, maximum %d",
          dbLimitsConfig.indexesNeededPerCollection(),
          saisUsed,
          dbLimitsConfig.indexesAvailablePerDatabase());
    }

    return null;
  }

  public static SimpleStatement getCreateTable(
      String keyspace,
      String table,
      boolean vectorSearch,
      int vectorSize,
      String comment,
      CollectionLexicalConfig lexicalConfig) {
    // The keyspace and table name are quoted to make it case-sensitive
    final String lexicalField = lexicalConfig.enabled() ? "    query_lexical_value   text, " : "";
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
              + lexicalField
              + "    PRIMARY KEY (key))";
      if (comment != null) {
        createTableWithVector = createTableWithVector + " WITH comment = '" + comment + "'";
      }
      return SimpleStatement.newInstance(String.format(createTableWithVector, keyspace, table));
    }
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
            + lexicalField
            + "    PRIMARY KEY (key))";
    if (comment != null) {
      createTable = createTable + " WITH comment = '" + comment + "'";
    }
    return SimpleStatement.newInstance(String.format(createTable, keyspace, table));
  }

  /*
   * When a createCollection is done on a table that already exist the index are run with IF NOT EXISTS.
   * For a new table they are run without IF NOT EXISTS.
   */
  public List<SimpleStatement> getIndexStatements(
      String keyspace,
      String table,
      CollectionLexicalConfig lexicalConfig,
      boolean collectionExisted) {
    List<SimpleStatement> statements = new ArrayList<>(10);
    String appender =
        collectionExisted ? "CREATE CUSTOM INDEX IF NOT EXISTS" : "CREATE CUSTOM INDEX";
    // All index names are quoted to make them case-sensitive.
    if (!indexingDenyAll()) {
      String existKeys =
          appender
              + " \"%s_exists_keys\" ON \"%s\".\"%s\" (exist_keys) USING 'StorageAttachedIndex'";

      statements.add(SimpleStatement.newInstance(String.format(existKeys, table, keyspace, table)));

      String arraySize =
          appender
              + " \"%s_array_size\" ON \"%s\".\"%s\" (entries(array_size)) USING 'StorageAttachedIndex'";
      statements.add(SimpleStatement.newInstance(String.format(arraySize, table, keyspace, table)));

      String arrayContains =
          appender
              + " \"%s_array_contains\" ON \"%s\".\"%s\" (array_contains) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(String.format(arrayContains, table, keyspace, table)));

      String boolQuery =
          appender
              + " \"%s_query_bool_values\" ON \"%s\".\"%s\" (entries(query_bool_values)) USING 'StorageAttachedIndex'";
      statements.add(SimpleStatement.newInstance(String.format(boolQuery, table, keyspace, table)));

      String dblQuery =
          appender
              + " \"%s_query_dbl_values\" ON \"%s\".\"%s\" (entries(query_dbl_values)) USING 'StorageAttachedIndex'";
      statements.add(SimpleStatement.newInstance(String.format(dblQuery, table, keyspace, table)));

      String textQuery =
          appender
              + " \"%s_query_text_values\" ON \"%s\".\"%s\" (entries(query_text_values)) USING 'StorageAttachedIndex'";
      statements.add(SimpleStatement.newInstance(String.format(textQuery, table, keyspace, table)));

      String timestampQuery =
          appender
              + " \"%s_query_timestamp_values\" ON \"%s\".\"%s\" (entries(query_timestamp_values)) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(String.format(timestampQuery, table, keyspace, table)));

      String nullQuery =
          appender
              + " \"%s_query_null_values\" ON \"%s\".\"%s\" (query_null_values) USING 'StorageAttachedIndex'";
      statements.add(SimpleStatement.newInstance(String.format(nullQuery, table, keyspace, table)));
    }

    if (vectorSearch) {
      String vectorSearch =
          appender
              + " \"%s_query_vector_value\" ON \"%s\".\"%s\" (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': '"
              + vectorFunction()
              + "', 'source_model': '"
              + sourceModel()
              + "'}";
      statements.add(
          SimpleStatement.newInstance(String.format(vectorSearch, table, keyspace, table)));
    }

    if (lexicalConfig.enabled()) {
      var analyzerDef = lexicalConfig.analyzerDefinition();
      // Note: needs to be either plain (unquoted) String (NOT quoted JSON String) OR JSON Object
      final String analyzerString =
          analyzerDef.isTextual() ? analyzerDef.asText() : analyzerDef.toString();
      final String lexicalCreateStmt =
              """
                    %s "%s_query_lexical_value" ON "%s"."%s" (query_lexical_value)
                      USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': '%s' }
                    """
              .formatted(appender, table, keyspace, table, analyzerString);
      statements.add(SimpleStatement.newInstance(lexicalCreateStmt));
    }
    return statements;
  }
}
