package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.FindEmbeddingProvidersOperation;
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
      throw ErrorCodeV1.VECTORIZE_FEATURE_NOT_AVAILABLE.toApiException();
    }
    return new FindEmbeddingProvidersOperation(
        embeddingProvidersConfig, operationsConfig.returnDeprecatedModels());
  }
}
