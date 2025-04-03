package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.reranking.FindRerankingProvidersOperation;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
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

    boolean isRerankingEnabledForAPI = ctx.apiFeatures().isFeatureEnabled(ApiFeature.RERANKING);
    if (!isRerankingEnabledForAPI) {
      throw ErrorCodeV1.RERANKING_FEATURE_NOT_ENABLED.toApiException();
    }

    return new FindRerankingProvidersOperation(rerankingProvidersConfig);
  }
}
