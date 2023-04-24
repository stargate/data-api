package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindCollectionsOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Command resolver for the {@link FindCollectionsCommand}. */
@ApplicationScoped
public class FindCollectionsCommandResolver implements CommandResolver<FindCollectionsCommand> {

  private final SchemaManager schemaManager;

  @Inject
  public FindCollectionsCommandResolver(SchemaManager schemaManager) {
    this.schemaManager = schemaManager;
  }

  /** {@inheritDoc} */
  @Override
  public Class<FindCollectionsCommand> getCommandClass() {
    return FindCollectionsCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveCommand(CommandContext ctx, FindCollectionsCommand command) {
    return new FindCollectionsOperation(schemaManager, ctx);
  }
}
