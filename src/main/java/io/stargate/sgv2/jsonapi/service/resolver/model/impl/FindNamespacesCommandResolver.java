package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindNamespacesOperations;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Command resolver for {@link FindNamespacesCommand}. */
@ApplicationScoped
public class FindNamespacesCommandResolver implements CommandResolver<FindNamespacesCommand> {

  private final SchemaManager schemaManager;

  @Inject
  public FindNamespacesCommandResolver(SchemaManager schemaManager) {
    this.schemaManager = schemaManager;
  }

  /** {@inheritDoc} */
  @Override
  public Class<FindNamespacesCommand> getCommandClass() {
    return FindNamespacesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveCommand(CommandContext ctx, FindNamespacesCommand command) {
    return new FindNamespacesOperations(schemaManager);
  }
}
