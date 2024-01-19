package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link CountDocumentsCommand } */
@ApplicationScoped
public class CountDocumentsCommandResolver extends FilterableResolver<CountDocumentsCommand>
    implements CommandResolver<CountDocumentsCommand> {

  private final OperationsConfig operationsConfig;

  @Inject
  public CountDocumentsCommandResolver(OperationsConfig operationsConfig) {
    super();
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<CountDocumentsCommand> getCommandClass() {
    return CountDocumentsCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CountDocumentsCommand command) {
    LogicalExpression logicalExpression = resolve(ctx, command);
    return new CountOperation(
        ctx,
        logicalExpression,
        operationsConfig.defaultCountPageSize(),
        operationsConfig.maxCountLimit());
  }
}
