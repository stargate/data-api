package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.rerank.FindRerankingProvidersOperation;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindRerankingProvidersCommand}. */
@ApplicationScoped
public class FindRerankingProvidersCommandResolver
    implements CommandResolver<FindRerankingProvidersCommand> {

  @Inject OperationsConfig operationsConfig;
  @Inject RerankingProvidersConfig rerankingProvidersConfig;

  public FindRerankingProvidersCommandResolver() {}

  @Override
  public Class<FindRerankingProvidersCommand> getCommandClass() {
    return FindRerankingProvidersCommand.class;
  }

  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, FindRerankingProvidersCommand command) {
    return new FindRerankingProvidersOperation(rerankingProvidersConfig);
  }
}
