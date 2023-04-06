package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindCollectionsOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.schema.CollectionManager;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Command resolver for the {@link FindCollectionsCommand}. */
@ApplicationScoped
public class FindCollectionsCommandResolver implements CommandResolver<FindCollectionsCommand> {

  private final CollectionManager collectionManager;

  @Inject
  public FindCollectionsCommandResolver(CollectionManager collectionManager) {
    this.collectionManager = collectionManager;
  }

  /** {@inheritDoc} */
  @Override
  public Class<FindCollectionsCommand> getCommandClass() {
    return FindCollectionsCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveCommand(CommandContext ctx, FindCollectionsCommand command) {
    return new FindCollectionsOperation(collectionManager, ctx);
  }
}
