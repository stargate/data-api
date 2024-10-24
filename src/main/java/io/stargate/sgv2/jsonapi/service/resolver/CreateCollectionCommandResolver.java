package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CreateCollectionCommandResolver implements CommandResolver<CreateCollectionCommand> {

  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;
  private final DocumentLimitsConfig documentLimitsConfig;
  private final DatabaseLimitsConfig dbLimitsConfig;
  private final OperationsConfig operationsConfig;
  private final VectorizeConfigValidator validateVectorize;

  @Inject
  public CreateCollectionCommandResolver(
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      DocumentLimitsConfig documentLimitsConfig,
      DatabaseLimitsConfig dbLimitsConfig,
      OperationsConfig operationsConfig,
      VectorizeConfigValidator validateVectorize) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
    this.documentLimitsConfig = documentLimitsConfig;
    this.dbLimitsConfig = dbLimitsConfig;
    this.operationsConfig = operationsConfig;
    this.validateVectorize = validateVectorize;
  }

  public CreateCollectionCommandResolver() {
    this(null, null, null, null, null, null);
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, CreateCollectionCommand command) {
    if (command.options() == null) {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          generateComment(objectMapper, false, false, command.name(), null, null, null),
          operationsConfig.databaseConfig().ddlDelayMillis(),
          operationsConfig.tooManyIndexesRollbackEnabled(),
          false); // Since the options is null
    }

    boolean hasIndexing = command.options().indexing() != null;
    boolean hasVectorSearch = command.options().vector() != null;
    CreateCollectionCommand.Options.VectorSearchConfig vector = command.options().vector();

    boolean indexingDenyAll = false;
    // handling indexing options
    if (hasIndexing) {
      // validation of configuration
      command.options().indexing().validate();
      indexingDenyAll = command.options().indexing().denyAll();
      // No need to process if both are null or empty
    }

    // handling vector option
    if (hasVectorSearch) {
      vector = validateVectorOptions(vector);
    }

    String comment =
        generateComment(
            objectMapper,
            hasIndexing,
            hasVectorSearch,
            command.name(),
            command.options().indexing(),
            vector,
            command.options().idConfig());

    if (hasVectorSearch) {
      return CreateCollectionOperation.withVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          vector.dimension(),
          vector.metric(),
          vector.sourceModel(),
          comment,
          operationsConfig.databaseConfig().ddlDelayMillis(),
          operationsConfig.tooManyIndexesRollbackEnabled(),
          indexingDenyAll);
    } else {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          comment,
          operationsConfig.databaseConfig().ddlDelayMillis(),
          operationsConfig.tooManyIndexesRollbackEnabled(),
          indexingDenyAll);
    }
  }

  /**
   * Generate a JSON string comment that will be stored in the database.
   *
   * @param hasIndexing indicating if indexing options are enabled.
   * @param hasVectorSearch indicating if vector search options are enabled.
   * @param commandName command name
   * @param indexing the indexing option config
   * @param vector vector config after validation
   * @return the comment string
   */
  public static String generateComment(
      ObjectMapper objectMapper,
      boolean hasIndexing,
      boolean hasVectorSearch,
      String commandName,
      CreateCollectionCommand.Options.IndexingConfig indexing,
      CreateCollectionCommand.Options.VectorSearchConfig vector,
      CreateCollectionCommand.Options.IdConfig idConfig) {
    final ObjectNode collectionNode = objectMapper.createObjectNode();
    ObjectNode optionsNode = objectMapper.createObjectNode(); // For storing collection options.

    // TODO: move this out of the command resolver, it is not a responsbility for this class
    if (hasIndexing) {
      optionsNode.putPOJO(TableCommentConstants.COLLECTION_INDEXING_KEY, indexing);
    }
    if (hasVectorSearch) {
      optionsNode.putPOJO(TableCommentConstants.COLLECTION_VECTOR_KEY, vector);
    }
    // if default_id is not specified during createCollection, resolve type to empty string
    if (idConfig != null) {
      optionsNode.putPOJO(TableCommentConstants.DEFAULT_ID_KEY, idConfig);
    } else {
      optionsNode.putPOJO(
          TableCommentConstants.DEFAULT_ID_KEY,
          objectMapper.createObjectNode().putPOJO("type", ""));
    }

    collectionNode.put(TableCommentConstants.COLLECTION_NAME_KEY, commandName);
    collectionNode.put(
        TableCommentConstants.SCHEMA_VERSION_KEY, TableCommentConstants.SCHEMA_VERSION_VALUE);
    collectionNode.putPOJO(TableCommentConstants.OPTIONS_KEY, optionsNode);
    final ObjectNode tableCommentNode = objectMapper.createObjectNode();
    tableCommentNode.putPOJO(TableCommentConstants.TOP_LEVEL_KEY, collectionNode);
    return tableCommentNode.toString();
  }

  /**
   * Validates the vector search options provided in a create collection command. It checks if
   * vector search is enabled globally, and validates the specific vectorization service
   * configuration provided by the user. It also ensures the specified vector dimension complies
   * with config limits.
   *
   * @param vector The vector search configuration provided by the user in the create collection
   *     command.
   * @return The validated and potentially modified (adding default vector dimension) vector search
   *     configuration.
   * @throws JsonApiException If vector search is disabled globally or the user configuration is
   *     invalid.
   */
  private CreateCollectionCommand.Options.VectorSearchConfig validateVectorOptions(
      CreateCollectionCommand.Options.VectorSearchConfig vector) {

    if (vector.vectorizeConfig() != null && !operationsConfig.vectorizeEnabled()) {
      throw ErrorCodeV1.VECTORIZE_FEATURE_NOT_AVAILABLE.toApiException();
    }

    Integer vectorDimension = vector.dimension();
    VectorizeConfig service = vector.vectorizeConfig();

    if (service != null) {
      // Validate service configuration and auto populate vector dimension.
      vectorDimension = validateVectorize.validateService(service, vectorDimension);
      vector =
          new CreateCollectionCommand.Options.VectorSearchConfig(
              vectorDimension, vector.metric(), vector.sourceModel(), vector.vectorizeConfig());
    } else {
      // Ensure vector dimension is provided when service configuration is absent.
      if (vectorDimension == null) {
        throw ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.toApiException(
            "The 'dimension' can not be null if 'service' is not provided");
      }
      if (vectorDimension > documentLimitsConfig.maxVectorEmbeddingLength()) {
        throw ErrorCodeV1.VECTOR_SEARCH_TOO_BIG_VALUE.toApiException(
            "%d (max %d)", vectorDimension, documentLimitsConfig.maxVectorEmbeddingLength());
      }
    }
    return vector;
  }
}
