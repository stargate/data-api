package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
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

  @Inject
  public CreateCollectionCommandResolver(
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      DataStoreConfig dataStoreConfig,
      DocumentLimitsConfig documentLimitsConfig,
      DatabaseLimitsConfig dbLimitsConfig) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
    this.dataStoreConfig = dataStoreConfig;
    this.documentLimitsConfig = documentLimitsConfig;
    this.dbLimitsConfig = dbLimitsConfig;
  }

  public CreateCollectionCommandResolver() {
    this(null, null, null, null, null);
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateCollectionCommand command) {

    if (command.options() != null) {
      String vectorize = null;
      String indexing = null;
      int vectorSize = 0;
      String function = null;

      // handling indexing options
      if (command.options().indexing() != null) {
        // validation of configuration
        command.options().indexing().validate();

        // No need to process if both are null or empty
        try {
          indexing = objectMapper.writeValueAsString(command.options().indexing());
        } catch (JsonProcessingException e) {
          // This should never happen because the object is extracted from json request
          throw new RuntimeException(e);
        }
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
              ErrorCode.VECTOR_SEARCH_FIELD_TOO_BIG,
              String.format(
                  "%s: %d (max %d)",
                  ErrorCode.VECTOR_SEARCH_FIELD_TOO_BIG.getMessage(),
                  vectorSize,
                  documentLimitsConfig.maxVectorEmbeddingLength()));
        }
        if (command.options().vectorize() != null) {
          try {
            vectorize = objectMapper.writeValueAsString(command.options().vectorize());
          } catch (JsonProcessingException e) {
            // This should never happen because the object is extracted from json request
            throw new RuntimeException(e);
          }
        }
      }

      String comment = null;
      if (indexing != null || vectorize != null) {
        final ObjectNode objectNode = objectMapper.createObjectNode();
        try {
          if (indexing != null) {
            objectNode.put("indexing", objectMapper.readTree(indexing));
          }
          if (vectorize != null) {
            objectNode.put("vectorize", objectMapper.readTree(vectorize));
          }
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
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
            comment);
      } else {
        return CreateCollectionOperation.withoutVectorSearch(
            ctx, dbLimitsConfig, objectMapper, cqlSessionCache, command.name(), comment);
      }
    } else {
      return CreateCollectionOperation.withoutVectorSearch(
          ctx, dbLimitsConfig, objectMapper, cqlSessionCache, command.name(), null);
    }
  }
}
