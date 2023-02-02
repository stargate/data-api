package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneAndUpdateCommand } */
@ApplicationScoped
public class FindOneAndUpdateCommandResolver extends FilterableResolver<FindOneAndUpdateCommand>
    implements CommandResolver<FindOneAndUpdateCommand> {
  private Shredder shredder;

  @Inject
  public FindOneAndUpdateCommandResolver(ObjectMapper objectMapper, Shredder shredder) {
    super(objectMapper, true, true);
    this.shredder = shredder;
  }

  @Override
  public Class<FindOneAndUpdateCommand> getCommandClass() {
    return FindOneAndUpdateCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, FindOneAndUpdateCommand command) {
    ReadOperation readOperation = resolve(ctx, command);
    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());
    return new ReadAndUpdateOperation(ctx, readOperation, documentUpdater, true, shredder);
  }

  @Override
  protected FilteringOptions getFilteringOption(FindOneAndUpdateCommand command) {
    return new FilteringOptions(1, null, 1);
  }
}
