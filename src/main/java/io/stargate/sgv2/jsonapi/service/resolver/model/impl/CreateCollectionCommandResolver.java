package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CreateCollectionCommandResolver implements CommandResolver<CreateCollectionCommand> {

  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;
  private final DataStoreConfig dataStoreConfig;

  private final DocumentLimitsConfig documentLimitsConfig;
  private final DatabaseLimitsConfig dbLimitsConfig;

  private final OperationsConfig operationsConfig;

  @Inject
  public CreateCollectionCommandResolver(
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      DataStoreConfig dataStoreConfig,
      DocumentLimitsConfig documentLimitsConfig,
      DatabaseLimitsConfig dbLimitsConfig,
      OperationsConfig operationsConfig) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
    this.dataStoreConfig = dataStoreConfig;
    this.documentLimitsConfig = documentLimitsConfig;
    this.dbLimitsConfig = dbLimitsConfig;
    this.operationsConfig = operationsConfig;
  }

  public CreateCollectionCommandResolver() {
    this(null, null, null, null, null, null);
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateCollectionCommand command) {

    if (command.options() != null) {
      boolean vectorize = false;
      boolean indexing = false;
      int vectorSize = 0;
      String function = null;

      // handling indexing options
      if (command.options().indexing() != null) {
        // validation of configuration
        command.options().indexing().validate();
        indexing = true;
        // No need to process if both are null or empty

      }

      // handling vector and vectorize options
      if (command.options().vector() != null) {
        if (!dataStoreConfig.vectorSearchEnabled()) {
          throw new JsonApiException(
              ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE,
              ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE.getMessage());
        }
        function = command.options().vector().metric();
        vectorSize = command.options().vector().dimension();
        if (vectorSize > documentLimitsConfig.maxVectorEmbeddingLength()) {
          throw new JsonApiException(
              ErrorCode.VECTOR_SEARCH_TOO_BIG_VALUE,
              String.format(
                  "%s: %d (max %d)",
                  ErrorCode.VECTOR_SEARCH_TOO_BIG_VALUE.getMessage(),
                  vectorSize,
                  documentLimitsConfig.maxVectorEmbeddingLength()));
        }
        if (command.options().vectorize() != null) {
          vectorize = true;
        }
      }

      String comment = null;
      if (indexing || vectorize) {
        final ObjectNode objectNode = objectMapper.createObjectNode();
        if (indexing) {
          objectNode.putPOJO("indexing", command.options().indexing());
        }
        if (vectorize) {
          objectNode.putPOJO("vectorize", command.options().vectorize());
        }
        comment = objectNode.toString();
      }

      if (command.options().vector() != null) {
        return CreateCollectionOperation.withVectorSearch(
            ctx,
            dbLimitsConfig,
            objectMapper,
            cqlSessionCache,
            command.name(),
            vectorSize,
            function,
            comment,
            operationsConfig.databaseConfig().ddlDelayMillis());
      } else {
        return CreateCollectionOperation.withoutVectorSearch(
            ctx,
            dbLimitsConfig,
            objectMapper,
            cqlSessionCache,
            command.name(),
            comment,
            operationsConfig.databaseConfig().ddlDelayMillis());
      }
    } else {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx,
          dbLimitsConfig,
          objectMapper,
          cqlSessionCache,
          command.name(),
          null,
          operationsConfig.databaseConfig().ddlDelayMillis());
    }
  }
}
