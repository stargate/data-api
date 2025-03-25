package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.reranking.FindRerankingProvidersOperation;
import io.stargate.sgv2.jsonapi.service.provider.reranking.configuration.RerankingProvidersConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindRerankingProvidersCommand}. */
@ApplicationScoped
public class FindRerankingProvidersCommandResolver
    implements CommandResolver<FindRerankingProvidersCommand> {

  @Inject RerankingProvidersConfig rerankingProvidersConfig;

  public FindRerankingProvidersCommandResolver() {}

  @Override
  public Class<FindRerankingProvidersCommand> getCommandClass() {
    return FindRerankingProvidersCommand.class;
  }

  @Override
  public Operation<DatabaseSchemaObject> resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, FindRerankingProvidersCommand command) {
    return new FindRerankingProvidersOperation(rerankingProvidersConfig);
  }
}
