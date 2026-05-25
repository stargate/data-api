package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaHolder;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDef;
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
    CQLSessionCache cqlSessionCache,
    CqlIdentifier collectionName,
    int ddlDelayMillis,
    boolean tooManyIndexesRollbackEnabled,
    // nullable
    CreateCollectionCommand.Options.DocIdDesc docIdDesc,
    // nullable
    CreateCollectionCommand.Options.IndexingDesc indexingDesc,
    // nullable
    CreateCollectionCommand.Options.VectorSearchDesc vectorDesc,
    SchemaHolder<CollectionLexicalDef> lexicalDef,
    SchemaHolder<CollectionRerankDef> rerankDef)
    implements Operation<CollectionSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateCollectionOperation.class);

  private static final CollectionTableMatcher COLLECTION_MATCHER = new CollectionTableMatcher();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    var initialTableComment = generateTableComment();
    LOGGER.info(
        "execute()- createCollection for identifier= {}.{}, initialTableComment={}",
        commandContext.schemaObject().identifier().keyspace(),
        collectionName,
        initialTableComment);

    return queryExecutor
        .getDriverMetadata(requestContext)
        .map(Metadata::getKeyspaces)
        .flatMap(
            allKeyspaces -> {

              // Step 1 - does the keyspace exist ?
              var targetKeyspace =
                  allKeyspaces.get(commandContext.schemaObject().identifier().keyspace());
              if (targetKeyspace == null) {
                return Uni.createFrom()
                    .failure(
                        SchemaException.Code.UNKNOWN_KEYSPACE.get(
                            errVars(commandContext.schemaObject())));
              }

              // Step 2 - is there an existing table and if not is there enough free indexes ?
              var existingTableMetadata =
                  findTableAndValidateLimits(allKeyspaces, targetKeyspace, collectionName);

              // Step 3 - create the collection if no existing table
              // use the runningValue() of lexicalDef this will either be the value from user or
              // default
              if (existingTableMetadata == null) {
                return executeCollectionCreation(
                    requestContext,
                    queryExecutor,
                    initialTableComment,
                    lexicalDef().runningValue(),
                    false);
              }

              // Step 4- Existing collection, check if the schema from the user is the same as the
              // existing
              // we need to merge in the current schema if the user did not specify anything
              var existingCollectionSettings =
                  CollectionSchemaObject.getCollectionSettings(
                      requestContext, existingTableMetadata, OBJECT_MAPPER);
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "execute() - existingCollectionSettings: {}", existingCollectionSettings);
              }

              // Need to resolve the vector settings so we can use them to create a full
              // representation
              // of what the new collection will look like
              var vectorModelName =
                  getOrDefault(
                      vectorDesc,
                      CreateCollectionCommand.Options.VectorSearchDesc::sourceModel,
                      null);
              var embeddingSourceModel =
                  EmbeddingSourceModel.fromApiNameOrDefault(vectorModelName)
                      .orElseThrow(
                          () ->
                              EmbeddingSourceModel.getUnknownSourceModelException(vectorModelName));

              var similarityFunctionName =
                  getOrDefault(
                      vectorDesc, CreateCollectionCommand.Options.VectorSearchDesc::metric, null);
              var similarityFunction =
                  SimilarityFunction.fromApiNameOrDefault(similarityFunctionName)
                      .orElseThrow(
                          () ->
                              SimilarityFunction.getUnknownFunctionException(
                                  similarityFunctionName));

              // OK, we know there is an existing collection, and it is different from the one we
              // already have.
              // So we will replace the lexical and rerank in the new one with the existing if the
              // user did not specify new values.
              // NOTE: FROM NOW ON WE NEED TO USE THE OVERRIDEN VALUE, (which may or may not be
              // actually overidden)
              var overrideLexicalDef =
                  lexicalDef()
                      .replaceIfMissing(existingCollectionSettings.lexicalDefSchemaValue())
                      .value();
              var overrideRerankDef =
                  rerankDef()
                      .replaceIfMissing(existingCollectionSettings.rerankDefSchemaValue())
                      .value();

              var overrideTableComment =
                  generateTableComment(overrideLexicalDef, overrideRerankDef);

              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute() - overrideTableComment: {}", overrideTableComment);
              }

              var newCollectionSettings =
                  CollectionSchemaObject.createCollectionSettings(
                      requestContext,
                      existingTableMetadata,
                      vectorDesc != null,
                      getOrDefault(
                          vectorDesc,
                          CreateCollectionCommand.Options.VectorSearchDesc::dimension,
                          0),
                      similarityFunction,
                      embeddingSourceModel,
                      overrideTableComment,
                      OBJECT_MAPPER);

              boolean settingsAreEqual = existingCollectionSettings.equals(newCollectionSettings);
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "execute() - settingsAreEqual: {}, newCollectionSettings={}",
                    settingsAreEqual,
                    newCollectionSettings);
              }

              if (settingsAreEqual) {
                return executeCollectionCreation(
                    requestContext,
                    queryExecutor,
                    overrideTableComment,
                    overrideLexicalDef.runningValue(),
                    true);
              }
              return Uni.createFrom()
                  .failure(
                      SchemaException.Code.EXISTING_COLLECTION_DIFFERENT_SETTINGS.get(
                          Map.of("collectionName", cqlIdentifierToMessageString(collectionName))));
            });
  }

  @VisibleForTesting
  String generateTableComment() {
    return generateTableComment(lexicalDef(), rerankDef());
  }

  @VisibleForTesting
  String generateTableComment(
      SchemaHolder<CollectionLexicalDef> overrideLexicalDef,
      SchemaHolder<CollectionRerankDef> overrideRerankDef) {

    var optionsNode = OBJECT_MAPPER.createObjectNode();

    if (indexingDesc != null) {
      optionsNode.putPOJO(TableCommentConstants.COLLECTION_INDEXING_KEY, indexingDesc);
    }
    if (vectorDesc != null) {
      optionsNode.putPOJO(TableCommentConstants.COLLECTION_VECTOR_KEY, vectorDesc);
    }
    // if default_id is not specified during createCollection, resolve type to empty string
    if (docIdDesc != null) {
      optionsNode.putPOJO(TableCommentConstants.DEFAULT_ID_KEY, docIdDesc);
    } else {
      optionsNode.putPOJO(
          TableCommentConstants.DEFAULT_ID_KEY,
          OBJECT_MAPPER.createObjectNode().putPOJO("type", ""));
    }

    // Take the running value, this will either be what the user gave us or the appropriate default
    optionsNode.putPOJO(
        TableCommentConstants.COLLECTION_LEXICAL_CONFIG_KEY, overrideLexicalDef.runningValue());
    optionsNode.putPOJO(
        TableCommentConstants.COLLECTION_RERANKING_CONFIG_KEY, overrideRerankDef.runningValue());

    var collectionNode = OBJECT_MAPPER.createObjectNode();
    collectionNode.put(
        TableCommentConstants.COLLECTION_NAME_KEY, cqlIdentifierToJsonKey(collectionName));
    collectionNode.put(
        TableCommentConstants.SCHEMA_VERSION_KEY,
        CollectionSchemaVersion.CURRENT_VERSION.toString());
    collectionNode.putPOJO(TableCommentConstants.OPTIONS_KEY, optionsNode);

    var tableCommentNode = OBJECT_MAPPER.createObjectNode();
    tableCommentNode.putPOJO(TableCommentConstants.TOP_LEVEL_KEY, collectionNode);

    return tableCommentNode.toString();
  }

  /**
   * execute collection creation and indexes creation
   *
   * @param requestContext DBRequestContext
   * @param queryExecutor QueryExecutor instance
   * @param collectionLexicalDef Lexical configuration for the collection
   * @param collectionExisted boolean that says if collection existed before
   * @return Uni<Supplier<CommandResult>>
   */
  private Uni<Supplier<CommandResult>> executeCollectionCreation(
      RequestContext requestContext,
      QueryExecutor queryExecutor,
      String tableComment,
      CollectionLexicalDef collectionLexicalDef,
      boolean collectionExisted) {

    final Uni<AsyncResultSet> execCreateTable =
        queryExecutor.executeCreateSchemaChange(
            requestContext,
            getCreateTable(
                commandContext.schemaObject().identifier().keyspace(),
                collectionName,
                vectorDesc != null,
                getOrDefault(
                    vectorDesc, CreateCollectionCommand.Options.VectorSearchDesc::dimension, 0),
                tableComment,
                collectionLexicalDef));

    final Uni<Boolean> indexResult =
        execCreateTable
            .onItem()
            .delayIt()
            .by(Duration.ofMillis(ddlDelayMillis > 0 ? ddlDelayMillis : 100))
            .onItem()
            .transformToUni(
                res -> {
                  if (res.wasApplied()) {
                    final List<SimpleStatement> indexStatements =
                        getIndexStatements(
                            commandContext.schemaObject().identifier().keyspace(),
                            collectionName,
                            collectionLexicalDef,
                            collectionExisted);
                    Multi<AsyncResultSet> indexResultMulti;
                    /*
                    CI will override ddlDelayMillis to 0 using `-Dstargate.jsonapi.operations.database-config.ddl-delay-millis=0`
                       to speed up the test execution
                       This is ok because CI is run as single cassandra instance and there is no need to wait for the schema changes to propagate
                    */

                    if (ddlDelayMillis == 0) {
                      indexResultMulti =
                          createIndexParallel(queryExecutor, requestContext, indexStatements);
                    } else {
                      indexResultMulti =
                          createIndexOrdered(queryExecutor, requestContext, indexStatements);
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
                // amorton - 13 jan 2026 - this is bad, the old code would swallow the error for
                // creating the table and indexes, will need to improve later.

                // table creation failure or index creation failure
                // HACK - remove when re-writing this class
                return commandResultSupplier(
                    DatabaseException.Code.CORRUPTED_COLLECTION_SCHEMA.get(
                        errVars(
                            commandContext.schemaObject(),
                            map ->
                                map.put(
                                    "errorMessage",
                                    "Collection creation failure (unable to create table)"))));
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
                return cleanUpCollectionFailedWithTooManyIndex(requestContext, queryExecutor);
              }

              if (error.getMessage().matches("Index .* already exists")) {
                // if index creation fails because index already exists
                return Uni.createFrom()
                    .item(
                        () ->
                            commandResultSupplier(
                                SchemaException.Code.EXISTING_INDEX_FOR_COLLECTION.get(
                                    errVars(commandContext.schemaObject()))));
              } else {
                // if index creation violates DB index limit and collection existed before,
                // will not drop the collection
                return Uni.createFrom()
                    .item(
                        () ->
                            commandResultSupplier(
                                SchemaException.Code.TOO_MANY_INDEXES_FOR_COLLECTION.get(
                                    errVars(
                                        commandContext.schemaObject(),
                                        map ->
                                            map.put(
                                                "indexesPerCollection",
                                                String.valueOf(
                                                    dbLimitsConfig
                                                        .indexesNeededPerCollection()))))));
              }
            });
  }

  private Supplier<CommandResult> commandResultSupplier(Throwable throwable) {
    return () ->
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP).addThrowable(throwable).build();
  }

  /** Create indexes for collections in ordered. This is to avoid schema change conflicts. */
  private Multi<AsyncResultSet> createIndexOrdered(
      QueryExecutor queryExecutor,
      RequestContext requestContext,
      List<SimpleStatement> indexStatements) {

    return Multi.createFrom()
        .items(indexStatements.stream())
        .onItem()
        .transformToUni(
            indexStatement ->
                queryExecutor.executeCreateSchemaChange(requestContext, indexStatement))
        .concatenate();
  }

  /** Create indexes for collections in parallel. Only used to speed up the CI actions. */
  private Multi<AsyncResultSet> createIndexParallel(
      QueryExecutor queryExecutor,
      RequestContext requestContext,
      List<SimpleStatement> indexStatements) {

    return Multi.createFrom()
        .items(indexStatements.stream())
        .onItem()
        .transformToUni(
            indexStatement ->
                queryExecutor.executeCreateSchemaChange(requestContext, indexStatement))
        .merge();
  }

  public Uni<Supplier<CommandResult>> cleanUpCollectionFailedWithTooManyIndex(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    // turning the name into asInternal() because  DeleteCollectionCollectionOperation stil uses
    // string
    DeleteCollectionCollectionOperation deleteCollectionCollectionOperation =
        new DeleteCollectionCollectionOperation(commandContext, collectionName.asInternal());

    // amorton - 13 jan  2026 - keeping the existing logic here, where the error was returning in
    // two situations
    // unsure how the second happens
    var exception =
        SchemaException.Code.TOO_MANY_INDEXES_FOR_COLLECTION.get(
            errVars(
                commandContext.schemaObject(),
                map ->
                    map.put(
                        "indexesPerCollection",
                        String.valueOf(dbLimitsConfig.indexesNeededPerCollection()))));
    return deleteCollectionCollectionOperation
        .execute(requestContext, queryExecutor)
        .onItem()
        .transform(
            res ->
                (Supplier<CommandResult>)
                    () ->
                        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
                            .addThrowable(exception)
                            .build())
        .onFailure()
        .recoverWithItem(
            e ->
                // This is unlikely to happen for delete collection though
                // Also return with TOO_MANY_INDEXES exception
                () ->
                    CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
                        .addThrowable(exception)
                        .build());
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
      CqlIdentifier tableName) {

    // First: do we already have a Table with the same name?
    for (TableMetadata table : currKeyspace.getTables().values()) {
      if (table.getName().equals(tableName)) {
        // If that is not a valid Data API collection, error out the createCollectionCommand
        if (!COLLECTION_MATCHER.test(table)) {
          throw SchemaException.Code.EXISTING_TABLE_NOT_DATA_API_COLLECTION.get(
              Map.of("tableName", cqlIdentifierToMessageString(tableName)));
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
      throw SchemaException.Code.TOO_MANY_COLLECTIONS.get(
          Map.of(
              "table",
              cqlIdentifierToMessageString(tableName),
              "collectionCount",
              String.valueOf(collectionCount),
              "collectionMaxCount",
              String.valueOf(MAX_COLLECTIONS)));
    }

    // And then see how many Indexes have been created, how many available
    int saisUsed = allTables.stream().mapToInt(table -> table.getIndexes().size()).sum();
    if ((saisUsed + dbLimitsConfig.indexesNeededPerCollection())
        > dbLimitsConfig.indexesAvailablePerDatabase()) {
      throw SchemaException.Code.TOO_MANY_INDEXES_FOR_COLLECTION.get(
          errVars(
              commandContext.schemaObject(),
              map ->
                  map.put(
                      "indexesPerCollection",
                      String.valueOf(dbLimitsConfig.indexesNeededPerCollection()))));
    }

    return null;
  }

  public static SimpleStatement getCreateTable(
      CqlIdentifier keyspace,
      CqlIdentifier table,
      boolean vectorSearch,
      int vectorSize,
      String comment,
      CollectionLexicalDef overrideLexicalDef) {

    // The keyspace and table name are quoted to make it case-sensitive
    final String lexicalField =
        overrideLexicalDef.enabled() ? "    query_lexical_value   text, " : "";
    if (vectorSearch) {
      // Quotes on identifiers come from cqlIdentifierToCQL
      String createTableWithVector =
          "CREATE TABLE IF NOT EXISTS %s.%s ("
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
      return SimpleStatement.newInstance(
          String.format(
              createTableWithVector, cqlIdentifierToCQL(keyspace), cqlIdentifierToCQL(table)));
    }
    // Quotes on identifiers come from cqlIdentifierToCQL
    String createTable =
        "CREATE TABLE IF NOT EXISTS %s.%s ("
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
    return SimpleStatement.newInstance(
        String.format(createTable, cqlIdentifierToCQL(keyspace), cqlIdentifierToCQL(table)));
  }

  /*
   * When a createCollection is done on a table that already exist the index are run with IF NOT EXISTS.
   * For a new table they are run without IF NOT EXISTS.
   */
  public List<SimpleStatement> getIndexStatements(
      CqlIdentifier keyspace,
      CqlIdentifier table,
      CollectionLexicalDef overrideLexicalDef,
      boolean collectionExisted) {

    List<SimpleStatement> statements = new ArrayList<>(10);

    String appender =
        collectionExisted ? "CREATE CUSTOM INDEX IF NOT EXISTS" : "CREATE CUSTOM INDEX";
    // All index names are quoted to make them case-sensitive.
    var denyAllIndexes =
        getOrDefault(indexingDesc, CreateCollectionCommand.Options.IndexingDesc::denyAll, false);

    if (!denyAllIndexes) {
      // Quotes on identifiers come from cqlIdentifierToCQL
      String existKeys =
          appender + " \"%s_exists_keys\" ON %s.%s (exist_keys) USING 'StorageAttachedIndex'";

      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  existKeys,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String arraySize =
          appender
              + " \"%s_array_size\" ON %s.%s (entries(array_size)) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  arraySize,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String arrayContains =
          appender
              + " \"%s_array_contains\" ON %s.%s (array_contains) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  arrayContains,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String boolQuery =
          appender
              + " \"%s_query_bool_values\" ON %s.%s (entries(query_bool_values)) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  boolQuery,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String dblQuery =
          appender
              + " \"%s_query_dbl_values\" ON %s.%s (entries(query_dbl_values)) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  dblQuery,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String textQuery =
          appender
              + " \"%s_query_text_values\" ON %s.%s (entries(query_text_values)) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  textQuery,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String timestampQuery =
          appender
              + " \"%s_query_timestamp_values\" ON %s.%s (entries(query_timestamp_values)) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  timestampQuery,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));

      String nullQuery =
          appender
              + " \"%s_query_null_values\" ON %s.%s (query_null_values) USING 'StorageAttachedIndex'";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  nullQuery,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));
    }

    if (vectorDesc != null) {
      String vectorSearch =
          appender
              + " \"%s_query_vector_value\" ON %s.%s (query_vector_value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': '"
              + vectorDesc.metric()
              + "', 'source_model': '"
              + vectorDesc.sourceModel()
              + "'}";
      statements.add(
          SimpleStatement.newInstance(
              String.format(
                  vectorSearch,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table))));
    }

    if (overrideLexicalDef.enabled()) {
      var analyzerDef = overrideLexicalDef.analyzerDefinition();
      // Note: needs to be either plain (unquoted) String (NOT quoted JSON String) OR JSON Object
      final String analyzerString =
          analyzerDef.isTextual() ? analyzerDef.asText() : analyzerDef.toString();
      // Quotes on identifiers come from cqlIdentifierToCQL
      final String lexicalCreateStmt =
              """
                    %s "%s_query_lexical_value" ON %s.%s (query_lexical_value)
                      USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': '%s' }
                    """
              .formatted(
                  appender,
                  table.asInternal(), // we want internal (without the quotes) for the name of the
                  // index
                  cqlIdentifierToCQL(keyspace),
                  cqlIdentifierToCQL(table),
                  analyzerString);
      statements.add(SimpleStatement.newInstance(lexicalCreateStmt));
    }
    return statements;
  }
}
