package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.FindEmbeddingProvidersOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindEmbeddingProvidersCommand}. */
@ApplicationScoped
public class FindEmbeddingProvidersCommandResolver
    implements CommandResolver<FindEmbeddingProvidersCommand> {

  @Inject EmbeddingProvidersConfig embeddingProvidersConfig;
  @Inject OperationsConfig operationsConfig;

  public FindEmbeddingProvidersCommandResolver() {}

  @Override
  public Class<FindEmbeddingProvidersCommand> getCommandClass() {
    return FindEmbeddingProvidersCommand.class;
  }

  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, FindEmbeddingProvidersCommand command) {
    if (!operationsConfig.vectorizeEnabled()) {
      throw ErrorCode.VECTORIZE_FEATURE_NOT_AVAILABLE.toApiException();
    }
    return new FindEmbeddingProvidersOperation(embeddingProvidersConfig);
  }
}
