package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankProvidersCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.rerank.FindRerankProvidersOperation;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindRerankProvidersCommand}. */
@ApplicationScoped
public class FindRerankProvidersCommandResolver
    implements CommandResolver<FindRerankProvidersCommand> {

  @Inject OperationsConfig operationsConfig;
  @Inject RerankProvidersConfig rerankProvidersConfig;

  public FindRerankProvidersCommandResolver() {}

  @Override
  public Class<FindRerankProvidersCommand> getCommandClass() {
    return FindRerankProvidersCommand.class;
  }

  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, FindRerankProvidersCommand command) {
    // TODO(Hazel): change the config check
    if (!operationsConfig.vectorizeEnabled()) {
      throw ErrorCodeV1.VECTORIZE_FEATURE_NOT_AVAILABLE.toApiException();
    }
    return new FindRerankProvidersOperation(rerankProvidersConfig);
  }
}
