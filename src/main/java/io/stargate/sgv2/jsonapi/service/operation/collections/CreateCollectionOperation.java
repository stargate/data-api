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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
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
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedCreateIndex;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
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
import io.stargate.sgv2.jsonapi.service.schema.tables.CQLSAIIndex;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a new collection in the target keyspace using the SuperShredding table model */
public class CreateCollectionOperation implements Operation<KeyspaceSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateCollectionOperation.class);

  private static final CollectionTableMatcher COLLECTION_MATCHER = new CollectionTableMatcher();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final CommandContext<KeyspaceSchemaObject> commandContext;
  private final DatabaseLimitsConfig dbLimitsConfig;
  private final CqlIdentifier collectionName;
  private final int ddlDelayMillis;
  private final boolean tooManyIndexesRollbackEnabled;
  // nullable
  private final CreateCollectionCommand.Options.DocIdDesc docIdDesc;
  // nullable
  private final CreateCollectionCommand.Options.IndexingDesc indexingDesc;
  // nullable
  private final CreateCollectionCommand.Options.VectorSearchDesc vectorDesc;
  private final SchemaHolder<CollectionLexicalDef> lexicalDef;
  private final SchemaHolder<CollectionRerankDef> rerankDef;

  public CreateCollectionOperation(
      CommandContext<KeyspaceSchemaObject> commandContext,
      DatabaseLimitsConfig dbLimitsConfig,
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
      SchemaHolder<CollectionRerankDef> rerankDef) {
    this.commandContext = commandContext;
    this.dbLimitsConfig = dbLimitsConfig;
    this.collectionName = collectionName;
    this.ddlDelayMillis = ddlDelayMillis;
    this.tooManyIndexesRollbackEnabled = tooManyIndexesRollbackEnabled;
    this.docIdDesc = docIdDesc;
    this.indexingDesc = indexingDesc;
    this.vectorDesc = vectorDesc;
    this.lexicalDef = lexicalDef;
    this.rerankDef = rerankDef;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public CqlIdentifier collectionName() {
    return collectionName;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public CommandContext<KeyspaceSchemaObject> commandContext() {
    return commandContext;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public CreateCollectionCommand.Options.DocIdDesc docIdDesc() {
    return docIdDesc;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public CreateCollectionCommand.Options.IndexingDesc indexingDesc() {
    return indexingDesc;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public CreateCollectionCommand.Options.VectorSearchDesc vectorDesc() {
    return vectorDesc;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public SchemaHolder<CollectionLexicalDef> lexicalDef() {
    return lexicalDef;
  }

  /**
   * Present and visible because the old testing relied on this being a record, keeping until we
   * review the testing in more detail and determine if needed.
   */
  @VisibleForTesting
  public SchemaHolder<CollectionRerankDef> rerankDef() {
    return rerankDef;
  }

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
                    lexicalDef.runningValue(),
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
                  lexicalDef
                      .replaceIfMissing(existingCollectionSettings.lexicalDefSchemaValue())
                      .value();
              var overrideRerankDef =
                  rerankDef
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
    return generateTableComment(lexicalDef, rerankDef);
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
    // Use ordinalValue() to get the integer representation of the enum into the JSON
    collectionNode.put(
        TableCommentConstants.SCHEMA_VERSION_KEY,
        CollectionSchemaVersion.CURRENT_VERSION.ordinalValue());
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
            requestContext, getCreateTable(tableComment, collectionLexicalDef));

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
                        getIndexStatements(collectionLexicalDef, collectionExisted);
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

  private SimpleStatement getCreateTable(String comment, CollectionLexicalDef overrideLexicalDef) {

    var keyspace = commandContext.schemaObject().identifier().keyspace();

    CreateTable create =
        SchemaBuilder.createTable(keyspace, collectionName)
            .ifNotExists()
            .withPartitionKey("key", DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT))
            .withColumn("tx_id", DataTypes.TIMEUUID)
            .withColumn("doc_json", DataTypes.TEXT)
            .withColumn("exist_keys", DataTypes.setOf(DataTypes.TEXT))
            .withColumn("array_size", DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT))
            .withColumn("array_contains", DataTypes.setOf(DataTypes.TEXT))
            .withColumn("query_bool_values", DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT))
            .withColumn("query_dbl_values", DataTypes.mapOf(DataTypes.TEXT, DataTypes.DECIMAL))
            .withColumn("query_text_values", DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT))
            .withColumn(
                "query_timestamp_values", DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIMESTAMP))
            .withColumn("query_null_values", DataTypes.setOf(DataTypes.TEXT));

    if (vectorDesc != null) {
      create =
          create.withColumn(
              "query_vector_value",
              new ExtendedVectorType(DataTypes.FLOAT, vectorDesc.dimension()));
    }
    if (overrideLexicalDef.enabled()) {
      create = create.withColumn("query_lexical_value", DataTypes.TEXT);
    }

    // adding the comment changes the return into something to deal with options
    var statement = comment == null ? create.build() : create.withComment(comment).build();

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("getCreateTable() - created table statement: {}", statement.getQuery());
    }
    return statement;
  }

  /*
   * When a createCollection is done on a table that already exist the index are run with IF NOT EXISTS.
   * For a new table they are run without IF NOT EXISTS.
   */
  private List<SimpleStatement> getIndexStatements(
      CollectionLexicalDef overrideLexicalDef, boolean collectionExisted) {

    List<SimpleStatement> statements = new ArrayList<>(10);

    var denyAllIndexes =
        getOrDefault(indexingDesc, CreateCollectionCommand.Options.IndexingDesc::denyAll, false);

    if (!denyAllIndexes) {
      statements.add(saiColumn(collectionExisted, "exists_keys", "exist_keys"));
      statements.add(saiEntries(collectionExisted, "array_size", "array_size"));
      statements.add(saiColumn(collectionExisted, "array_contains", "array_contains"));
      statements.add(saiEntries(collectionExisted, "query_bool_values", "query_bool_values"));
      statements.add(saiEntries(collectionExisted, "query_dbl_values", "query_dbl_values"));
      statements.add(saiEntries(collectionExisted, "query_text_values", "query_text_values"));
      statements.add(
          saiEntries(collectionExisted, "query_timestamp_values", "query_timestamp_values"));
      statements.add(saiColumn(collectionExisted, "query_null_values", "query_null_values"));
    }

    // NOTE: This is a little sloppy, in normal request the CreateCollectionCommandResolver will
    // make sure the vectorDesc is valid and has defaults set. So even though they are strings
    // they have been validated as the thing we should use. See
    // CreateCollectionCommandResolver.validateVectorOptions()
    // it gets the proper CQL names from the Enums, replacing what the user sent in. (kind of
    // confusing)
    // TODO: create a VectorSearchDef that uses the SimilarityFunction and EmbeddingSourceModel
    // enums
    if (vectorDesc != null) {
      // Sanity checking here, if we pass a null value the map go bang, try to stop bang, bang bad
      Map<String, Object> vectorOptions = new HashMap<>();
      if (vectorDesc.metric() != null && !vectorDesc.metric().isBlank()) {
        vectorOptions.put("similarity_function", vectorDesc.metric());
      }
      if (vectorDesc.sourceModel() != null && !vectorDesc.sourceModel().isBlank()) {
        vectorOptions.put("source_model", vectorDesc.sourceModel());
      }
      statements.add(
          buildSaiIndex(
              collectionExisted, "query_vector_value", "query_vector_value", false, vectorOptions));
    }

    if (overrideLexicalDef.enabled()) {
      var analyzerDef = overrideLexicalDef.analyzerDefinition();
      var analyzerString = analyzerDef.isTextual() ? analyzerDef.asText() : analyzerDef.toString();
      statements.add(
          buildSaiIndex(
              collectionExisted,
              "query_lexical_value",
              "query_lexical_value",
              false,
              Map.of("index_analyzer", analyzerString)));
    }

    if (LOGGER.isTraceEnabled()) {
      var cqlStrings =
          statements.stream().map(SimpleStatement::getQuery).collect(Collectors.joining("; "));
      LOGGER.trace("getIndexStatements() - created index statements: {}", cqlStrings);
    }
    return statements;
  }

  private SimpleStatement saiColumn(boolean ifNotExists, String indexSuffix, String column) {
    return buildSaiIndex(ifNotExists, indexSuffix, column, false, Map.of());
  }

  private SimpleStatement saiEntries(boolean ifNotExists, String indexSuffix, String column) {
    return buildSaiIndex(ifNotExists, indexSuffix, column, true, Map.of());
  }

  private SimpleStatement buildSaiIndex(
      boolean ifNotExists,
      String indexSuffix,
      String columnName, // aaron - next change will make this a CQLIdentifier
      boolean isEntries,
      Map<String, Object> options) {

    var keyspace = commandContext.schemaObject().identifier().keyspace();
    var index = CqlIdentifier.fromInternal(collectionName.asInternal() + "_" + indexSuffix);
    var column = CqlIdentifier.fromInternal(columnName);

    var start = SchemaBuilder.createIndex(index).custom(CQLSAIIndex.SAI_CLASS_NAME);
    if (ifNotExists) {
      start = start.ifNotExists();
    }

    var onTable = start.onTable(keyspace, collectionName);
    var createIndex = isEntries ? onTable.andColumnEntries(column) : onTable.andColumn(column);

    if (!options.isEmpty()) {
      // in the CQL statement OPTIONS are the things after WITH, and for the `create index` there is
      // an option called OPTIONS calling withSASIOptions deals with this.
      createIndex = createIndex.withSASIOptions(options);
    }

    return new ExtendedCreateIndex((DefaultCreateIndex) createIndex).build();
  }
}
