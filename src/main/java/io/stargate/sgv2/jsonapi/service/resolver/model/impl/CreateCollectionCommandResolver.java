package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CreateCollectionCommandResolver implements CommandResolver<CreateCollectionCommand> {
  private final DataStoreConfig dataStoreConfig;

  private final DocumentLimitsConfig documentLimitsConfig;

  @Inject
  public CreateCollectionCommandResolver(
      DataStoreConfig dataStoreConfig, DocumentLimitsConfig documentLimitsConfig) {
    this.dataStoreConfig = dataStoreConfig;
    this.documentLimitsConfig = documentLimitsConfig;
  }

  public CreateCollectionCommandResolver() {
    this(null, null);
  }

  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateCollectionCommand command) {
    if (command.options() != null && command.options().vector() != null) {
      if (!dataStoreConfig.vectorSearchEnabled()) {
        throw new JsonApiException(
            ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE,
            ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE.getMessage());
      }
      final int vectorSize = command.options().vector().size();
      if (vectorSize > documentLimitsConfig.maxVectorEmbeddingLength()) {
        throw new JsonApiException(
            ErrorCode.VECTOR_SEARCH_FIELD_TOO_BIG,
            String.format(
                "%s: %d (max %d)",
                ErrorCode.VECTOR_SEARCH_FIELD_TOO_BIG.getMessage(),
                vectorSize,
                documentLimitsConfig.maxVectorEmbeddingLength()));
      }
      return CreateCollectionOperation.withVectorSearch(
          ctx, command.name(), vectorSize, command.options().vector().function());
    } else {
      return CreateCollectionOperation.withoutVectorSearch(ctx, command.name());
    }
  }
}
