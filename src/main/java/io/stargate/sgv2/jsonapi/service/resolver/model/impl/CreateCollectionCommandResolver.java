package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
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

  @Inject
  public CreateCollectionCommandResolver(DataStoreConfig dataStoreConfig) {
    this.dataStoreConfig = dataStoreConfig;
  }

  public CreateCollectionCommandResolver() {
    this(null);
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
      return CreateCollectionOperation.withVectorSearch(
          ctx,
          command.name(),
          command.options().vector().size(),
          command.options().vector().function());
    } else {
      return CreateCollectionOperation.withoutVectorSearch(ctx, command.name());
    }
  }
}
