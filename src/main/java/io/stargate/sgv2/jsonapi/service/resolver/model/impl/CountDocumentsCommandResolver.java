package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link CountDocumentsCommands } */
@ApplicationScoped
public class CountDocumentsCommandResolver extends FilterableResolver<CountDocumentsCommands>
    implements CommandResolver<CountDocumentsCommands> {
  @Inject
  public CountDocumentsCommandResolver() {
    super();
  }

  @Override
  public Class<CountDocumentsCommands> getCommandClass() {
    return CountDocumentsCommands.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CountDocumentsCommands command) {
    List<DBFilterBase> filters = resolve(ctx, command);
    return new CountOperation(ctx, filters);
  }
}
